import discord
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional

from redbot.core import commands, Config, checks
from redbot.core.bot import Red
from discord.ext import tasks

from .database import FamilyDatabase
from .views import (
    ProposalView,
    SireProposalView,
    RunawaySelectView,
    PersistentProposalView,
    PersistentSireView,
)
from .visualization import FamilyTreeVisualizer

log = logging.getLogger("red.DurkCogs.Family")


class Family(commands.Cog):
    """
    A comprehensive family system with marriage, adoption, and family trees.

    Create relationships through marriage and adoption, view family trees,
    and manage your virtual family across Discord servers.
    """

    DEFAULT_GLOBAL = {
        "polyamory_enabled": False,
        "incest_enabled": False,
        "proposal_timeout": 300,
        "max_spouses": 5,
        "max_children": 10,
    }

    DEFAULT_GUILD = {
        "override_polyamory": None,
        "override_incest": None,
        "override_proposal_timeout": None,
    }

    def __init__(self, bot: Red):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=8675309420, force_registration=True)
        self.config.register_global(**self.DEFAULT_GLOBAL)
        self.config.register_guild(**self.DEFAULT_GUILD)

        self.db_path = Path(__file__).parent / "family.db"
        self.db: Optional[FamilyDatabase] = None
        self.visualizer = FamilyTreeVisualizer()

        # Start background task for proposal cleanup
        self.cleanup_proposals_task.start()

    async def cog_load(self):
        """Initialize database and register persistent views when cog loads."""
        self.db = FamilyDatabase(self.db_path)
        await self.db.initialize()

        # Register persistent views for button handling after restart
        self.bot.add_view(PersistentProposalView(self))
        self.bot.add_view(PersistentSireView(self))

        log.info("Family cog loaded and database initialized.")

    async def cog_unload(self):
        """Cleanup when cog unloads."""
        self.cleanup_proposals_task.cancel()
        if self.db:
            await self.db.close()
        log.info("Family cog unloaded.")

    @tasks.loop(minutes=1)
    async def cleanup_proposals_task(self):
        """Clean up expired proposals."""
        if not self.db:
            return

        try:
            expired = await self.db.get_expired_proposals()
            for proposal in expired:
                try:
                    channel = self.bot.get_channel(proposal["channel_id"])
                    if channel:
                        try:
                            message = await channel.fetch_message(proposal["message_id"])
                            embed = discord.Embed(
                                title="Proposal Expired",
                                description="This proposal has expired due to no response.",
                                color=discord.Color.greyple()
                            )
                            await message.edit(embed=embed, view=None)
                        except discord.NotFound:
                            pass
                        except discord.Forbidden:
                            pass
                    await self.db.delete_proposal(proposal["id"])
                except Exception as e:
                    log.error(f"Error cleaning up proposal {proposal['id']}: {e}")
        except Exception as e:
            log.error(f"Error in cleanup task: {e}")

    @cleanup_proposals_task.before_loop
    async def before_cleanup_task(self):
        await self.bot.wait_until_ready()

    # === Helper Methods ===

    async def get_effective_setting(self, guild_id: int, setting: str):
        """
        Get effective setting considering guild overrides.

        Args:
            guild_id: The guild ID
            setting: The setting name (e.g., 'polyamory', 'incest', 'proposal_timeout')
        """
        override_key = f"override_{setting}"
        # Map setting names to global config keys
        global_key_map = {
            "polyamory": "polyamory_enabled",
            "incest": "incest_enabled",
            "proposal_timeout": "proposal_timeout",
        }
        global_key = global_key_map.get(setting, setting)
        guild_val = await self.config.guild_from_id(guild_id).get_attr(override_key)()
        if guild_val is not None:
            return guild_val
        return await self.config.get_attr(global_key)()

    async def _validate_marriage(
        self, ctx: commands.Context, user: discord.Member
    ) -> Optional[str]:
        """Validate a marriage proposal. Returns error message or None if valid."""
        if user.bot:
            return "You can't marry a bot!"

        if user.id == ctx.author.id:
            return "You can't marry yourself!"

        if await self.db.are_married(ctx.author.id, user.id):
            return f"You're already married to {user.display_name}!"

        # Check polyamory
        polyamory = await self.get_effective_setting(ctx.guild.id, "polyamory")
        if not polyamory:
            author_spouses = await self.db.get_marriage_count(ctx.author.id)
            if author_spouses > 0:
                return "You're already married! (Polyamory is disabled on this server)"

            target_spouses = await self.db.get_marriage_count(user.id)
            if target_spouses > 0:
                return f"{user.display_name} is already married! (Polyamory is disabled on this server)"
        else:
            max_spouses = await self.config.max_spouses()
            author_spouses = await self.db.get_marriage_count(ctx.author.id)
            if author_spouses >= max_spouses:
                return f"You've reached the maximum number of spouses ({max_spouses})!"

        # Check incest
        incest = await self.get_effective_setting(ctx.guild.id, "incest")
        if not incest:
            if await self.db.are_related(ctx.author.id, user.id):
                return f"You can't marry {user.display_name} - you're related! (Incest is disabled on this server)"

        # Check for pending proposal
        if await self.db.has_pending_proposal(ctx.author.id, user.id, "marriage"):
            return f"You already have a pending marriage proposal to {user.display_name}!"

        return None

    async def _validate_adoption(
        self, ctx: commands.Context, child: discord.Member
    ) -> Optional[str]:
        """Validate an adoption proposal. Returns error message or None if valid."""
        if child.bot:
            return "You can't adopt a bot!"

        if child.id == ctx.author.id:
            return "You can't adopt yourself!"

        # Check if already parent
        if await self.db.is_parent_of(ctx.author.id, child.id):
            return f"You're already a parent of {child.display_name}!"

        # Check if child already has 2 parents
        parent_count = await self.db.get_parent_count(child.id)
        if parent_count >= 2:
            return f"{child.display_name} already has 2 parents!"

        # Check max children
        max_children = await self.config.max_children()
        current_children = len(await self.db.get_children(ctx.author.id))
        if current_children >= max_children:
            return f"You've reached the maximum number of children ({max_children})!"

        # Check incest - can't adopt your parent or spouse
        incest = await self.get_effective_setting(ctx.guild.id, "incest")
        if not incest:
            # Can't adopt your parent
            if child.id in await self.db.get_parents(ctx.author.id):
                return f"You can't adopt your own parent!"
            # Can't adopt your spouse (unless incest enabled)
            if await self.db.are_married(ctx.author.id, child.id):
                return f"You can't adopt your spouse! (Incest is disabled on this server)"

        # Check for pending proposal
        if await self.db.has_pending_proposal(ctx.author.id, child.id, "adoption"):
            return f"You already have a pending adoption proposal to {child.display_name}!"

        return None

    # === Proposal Handlers ===

    async def handle_marriage_accept(self, interaction: discord.Interaction, proposal_id: int):
        """Handle marriage proposal acceptance."""
        proposal = await self.db.get_proposal(proposal_id)
        if not proposal:
            await interaction.response.send_message(
                "This proposal no longer exists.",
                ephemeral=True
            )
            return

        proposer_id = proposal["proposer_id"]
        target_id = proposal["target_id"]

        # Create the marriage
        await self.db.create_marriage(proposer_id, target_id)
        await self.db.delete_proposal(proposal_id)

        proposer = self.bot.get_user(proposer_id)
        target = self.bot.get_user(target_id)

        proposer_name = proposer.display_name if proposer else f"User {proposer_id}"
        target_name = target.display_name if target else f"User {target_id}"

        embed = discord.Embed(
            title="\U0001f492 Marriage Announcement! \U0001f492",
            description=f"**{proposer_name}** and **{target_name}** are now married!",
            color=discord.Color.magenta()
        )
        embed.set_footer(text="Congratulations to the happy couple!")

        await interaction.response.edit_message(embed=embed, view=None)

    async def handle_adoption_accept(self, interaction: discord.Interaction, proposal_id: int):
        """Handle adoption proposal acceptance."""
        proposal = await self.db.get_proposal(proposal_id)
        if not proposal:
            await interaction.response.send_message(
                "This proposal no longer exists.",
                ephemeral=True
            )
            return

        parent_id = proposal["proposer_id"]
        child_id = proposal["target_id"]

        # Create the parent-child relationship
        await self.db.create_parent_child(parent_id, child_id, "adoption")
        await self.db.delete_proposal(proposal_id)

        parent = self.bot.get_user(parent_id)
        child = self.bot.get_user(child_id)

        parent_name = parent.display_name if parent else f"User {parent_id}"
        child_name = child.display_name if child else f"User {child_id}"

        embed = discord.Embed(
            title="\U0001f476 Adoption Announcement! \U0001f476",
            description=f"**{parent_name}** has adopted **{child_name}**!",
            color=discord.Color.green()
        )
        embed.set_footer(text="Welcome to the family!")

        await interaction.response.edit_message(embed=embed, view=None)

    async def handle_sire_complete(self, interaction: discord.Interaction, proposal_id: int):
        """Handle sire proposal completion (both parties accepted)."""
        proposal = await self.db.get_proposal(proposal_id)
        if not proposal:
            await interaction.response.send_message(
                "This proposal no longer exists.",
                ephemeral=True
            )
            return

        proposer_id = proposal["proposer_id"]
        coparent_id = proposal["target_id"]
        child_id = proposal["child_id"]

        # Check if proposer is already a parent, if not add them too
        proposer_is_parent = await self.db.is_parent_of(proposer_id, child_id)
        if not proposer_is_parent:
            await self.db.create_parent_child(proposer_id, child_id, "sire")

        # Create the parent-child relationship for the co-parent
        await self.db.create_parent_child(coparent_id, child_id, "sire")
        await self.db.delete_proposal(proposal_id)

        proposer = self.bot.get_user(proposer_id)
        coparent = self.bot.get_user(coparent_id)
        child = self.bot.get_user(child_id)

        proposer_name = proposer.display_name if proposer else f"User {proposer_id}"
        coparent_name = coparent.display_name if coparent else f"User {coparent_id}"
        child_name = child.display_name if child else f"User {child_id}"

        embed = discord.Embed(
            title="\U0001f46a Family Formed! \U0001f46a",
            description=f"**{proposer_name}** and **{coparent_name}** are now parents of **{child_name}**!",
            color=discord.Color.blue()
        )

        await interaction.response.edit_message(embed=embed, view=None)

    async def handle_proposal_decline(
        self, interaction: discord.Interaction, proposal_id: int, proposal_type: str
    ):
        """Handle proposal decline."""
        proposal = await self.db.get_proposal(proposal_id)
        if not proposal:
            await interaction.response.send_message(
                "This proposal no longer exists.",
                ephemeral=True
            )
            return

        await self.db.delete_proposal(proposal_id)

        proposer = self.bot.get_user(proposal["proposer_id"])
        proposer_name = proposer.display_name if proposer else f"User {proposal['proposer_id']}"

        type_text = {
            "marriage": "marriage proposal",
            "adoption": "adoption request",
            "sire": "co-parenting request"
        }.get(proposal_type, "proposal")

        embed = discord.Embed(
            title="Proposal Declined",
            description=f"The {type_text} from **{proposer_name}** was declined.",
            color=discord.Color.red()
        )

        await interaction.response.edit_message(embed=embed, view=None)

    async def handle_proposal_timeout(self, proposal_id: int):
        """Handle proposal timeout (called from view timeout)."""
        proposal = await self.db.get_proposal(proposal_id)
        if not proposal:
            return

        try:
            channel = self.bot.get_channel(proposal["channel_id"])
            if channel:
                try:
                    message = await channel.fetch_message(proposal["message_id"])
                    embed = discord.Embed(
                        title="Proposal Expired",
                        description="This proposal has expired due to no response.",
                        color=discord.Color.greyple()
                    )
                    await message.edit(embed=embed, view=None)
                except (discord.NotFound, discord.Forbidden):
                    pass
        except Exception as e:
            log.error(f"Error handling proposal timeout: {e}")

        await self.db.delete_proposal(proposal_id)

    async def execute_runaway(
        self, interaction: discord.Interaction, child_id: int, parent_id: int
    ):
        """Execute the runaway action."""
        success = await self.db.delete_parent_child(parent_id, child_id)

        if not success:
            await interaction.response.send_message(
                "Failed to run away - relationship not found.",
                ephemeral=True
            )
            return

        parent = self.bot.get_user(parent_id)
        parent_name = parent.display_name if parent else f"User {parent_id}"

        embed = discord.Embed(
            title="\U0001f3c3 Ran Away!",
            description=f"You have run away from **{parent_name}**!",
            color=discord.Color.orange()
        )

        await interaction.response.edit_message(embed=embed, view=None)

    # === Marriage Commands ===

    @commands.command()
    @commands.guild_only()
    async def marry(self, ctx: commands.Context, user: discord.Member):
        """Propose marriage to another user."""
        error = await self._validate_marriage(ctx, user)
        if error:
            await ctx.send(error)
            return

        timeout = await self.get_effective_setting(ctx.guild.id, "proposal_timeout")
        expires_at = (datetime.utcnow() + timedelta(seconds=timeout)).timestamp()

        embed = discord.Embed(
            title="\U0001f48d Marriage Proposal! \U0001f48d",
            description=f"**{ctx.author.display_name}** is proposing to **{user.display_name}**!\n\n"
                        f"{user.mention}, do you accept?",
            color=discord.Color.magenta()
        )
        embed.set_footer(text=f"This proposal expires in {timeout // 60} minutes")

        # Create view first (we'll set proposal_id after getting message)
        view = ProposalView(self, 0, user.id, "marriage", timeout=float(timeout))

        # Send message with view attached
        message = await ctx.send(embed=embed, view=view)

        # Create proposal in database
        proposal_id = await self.db.create_proposal(
            proposal_type="marriage",
            proposer_id=ctx.author.id,
            target_id=user.id,
            message_id=message.id,
            channel_id=ctx.channel.id,
            guild_id=ctx.guild.id,
            expires_at=expires_at
        )

        # Update view with proposal ID
        view.proposal_id = proposal_id

    @commands.command()
    @commands.guild_only()
    async def divorce(self, ctx: commands.Context, user: discord.Member):
        """Divorce your spouse."""
        if not await self.db.are_married(ctx.author.id, user.id):
            await ctx.send(f"You're not married to {user.display_name}!")
            return

        success = await self.db.delete_marriage(ctx.author.id, user.id)

        if success:
            embed = discord.Embed(
                title="\U0001f494 Divorce",
                description=f"**{ctx.author.display_name}** and **{user.display_name}** are no longer married.",
                color=discord.Color.dark_gray()
            )
            await ctx.send(embed=embed)
        else:
            await ctx.send("Something went wrong processing the divorce.")

    # === Adoption Commands ===

    @commands.command()
    @commands.guild_only()
    async def adopt(self, ctx: commands.Context, user: discord.Member):
        """Adopt another user as your child."""
        error = await self._validate_adoption(ctx, user)
        if error:
            await ctx.send(error)
            return

        timeout = await self.get_effective_setting(ctx.guild.id, "proposal_timeout")
        expires_at = (datetime.utcnow() + timedelta(seconds=timeout)).timestamp()

        embed = discord.Embed(
            title="\U0001f476 Adoption Request! \U0001f476",
            description=f"**{ctx.author.display_name}** wants to adopt **{user.display_name}**!\n\n"
                        f"{user.mention}, do you accept?",
            color=discord.Color.green()
        )
        embed.set_footer(text=f"This request expires in {timeout // 60} minutes")

        # Create view first
        view = ProposalView(self, 0, user.id, "adoption", timeout=float(timeout))

        # Send with view attached
        message = await ctx.send(embed=embed, view=view)

        proposal_id = await self.db.create_proposal(
            proposal_type="adoption",
            proposer_id=ctx.author.id,
            target_id=user.id,
            message_id=message.id,
            channel_id=ctx.channel.id,
            guild_id=ctx.guild.id,
            expires_at=expires_at
        )

        # Update view with proposal ID
        view.proposal_id = proposal_id

    @commands.command()
    @commands.guild_only()
    async def disown(self, ctx: commands.Context, user: discord.Member):
        """Disown your child."""
        if not await self.db.is_parent_of(ctx.author.id, user.id):
            await ctx.send(f"{user.display_name} is not your child!")
            return

        success = await self.db.delete_parent_child(ctx.author.id, user.id)

        if success:
            embed = discord.Embed(
                title="\U0001f6aa Disowned",
                description=f"**{ctx.author.display_name}** has disowned **{user.display_name}**.",
                color=discord.Color.dark_gray()
            )
            await ctx.send(embed=embed)
        else:
            await ctx.send("Something went wrong.")

    @commands.command()
    @commands.guild_only()
    async def runaway(self, ctx: commands.Context):
        """Run away from one of your parents."""
        parents = await self.db.get_parents(ctx.author.id)

        if not parents:
            await ctx.send("You don't have any parents to run away from!")
            return

        if len(parents) == 1:
            # Only one parent, auto-select
            parent_id = parents[0]
            success = await self.db.delete_parent_child(parent_id, ctx.author.id)

            if success:
                parent = self.bot.get_user(parent_id)
                parent_name = parent.display_name if parent else f"User {parent_id}"
                embed = discord.Embed(
                    title="\U0001f3c3 Ran Away!",
                    description=f"You have run away from **{parent_name}**!",
                    color=discord.Color.orange()
                )
                await ctx.send(embed=embed)
            else:
                await ctx.send("Something went wrong.")
            return

        # Multiple parents - show selection
        parent_data = []
        for parent_id in parents:
            parent = self.bot.get_user(parent_id)
            parent_name = parent.display_name if parent else f"User {parent_id}"
            parent_data.append({"id": parent_id, "name": parent_name})

        embed = discord.Embed(
            title="\U0001f3c3 Run Away",
            description="Select which parent you want to run away from:",
            color=discord.Color.orange()
        )

        view = RunawaySelectView(self, ctx.author.id, parent_data)
        await ctx.send(embed=embed, view=view)

    @commands.command()
    @commands.guild_only()
    async def sire(self, ctx: commands.Context, coparent: discord.Member, child: discord.Member):
        """
        Declare yourself and a co-parent as parents of a child.

        Both the co-parent and child must accept.
        If you're already a parent, this adds a co-parent.
        If neither of you are parents, both become parents.
        """
        # Validation
        if coparent.bot or child.bot:
            await ctx.send("Bots cannot be part of family relationships!")
            return

        if coparent.id == ctx.author.id:
            await ctx.send("You can't be your own co-parent!")
            return

        if child.id == ctx.author.id or child.id == coparent.id:
            await ctx.send("The child must be a different person!")
            return

        # Check current parent situation
        author_is_parent = await self.db.is_parent_of(ctx.author.id, child.id)
        coparent_is_parent = await self.db.is_parent_of(coparent.id, child.id)

        if author_is_parent and coparent_is_parent:
            await ctx.send(f"You and {coparent.display_name} are already both parents of {child.display_name}!")
            return

        # Check if child already has 2 parents
        parent_count = await self.db.get_parent_count(child.id)
        if parent_count >= 2:
            await ctx.send(f"{child.display_name} already has 2 parents!")
            return

        # If neither is a parent but child has 1 parent, can't add 2 more
        if not author_is_parent and not coparent_is_parent and parent_count == 1:
            await ctx.send(f"{child.display_name} already has a parent. Use `.adopt` to become their second parent.")
            return

        # Check incest
        incest = await self.get_effective_setting(ctx.guild.id, "incest")
        if not incest:
            if await self.db.are_related(coparent.id, child.id):
                await ctx.send(
                    f"{coparent.display_name} and {child.display_name} are already related! "
                    "(Incest is disabled on this server)"
                )
                return

        timeout = await self.get_effective_setting(ctx.guild.id, "proposal_timeout")
        expires_at = (datetime.utcnow() + timedelta(seconds=timeout)).timestamp()

        embed = discord.Embed(
            title="\U0001f46a Co-Parenting Request! \U0001f46a",
            description=f"**{ctx.author.display_name}** wants **{coparent.display_name}** "
                        f"to become a co-parent of **{child.display_name}**!\n\n"
                        f"Both {coparent.mention} and {child.mention} must accept.",
            color=discord.Color.blue()
        )
        embed.add_field(
            name="Acceptance Status",
            value=f"\u23f3 {coparent.display_name} (Co-parent)\n\u23f3 {child.display_name} (Child)",
            inline=False
        )
        embed.set_footer(text=f"This request expires in {timeout // 60} minutes")

        # Create view first
        view = SireProposalView(self, 0, coparent.id, child.id, timeout=float(timeout))

        # Send with view attached
        message = await ctx.send(embed=embed, view=view)

        proposal_id = await self.db.create_proposal(
            proposal_type="sire",
            proposer_id=ctx.author.id,
            target_id=coparent.id,
            child_id=child.id,
            message_id=message.id,
            channel_id=ctx.channel.id,
            guild_id=ctx.guild.id,
            expires_at=expires_at
        )

        # Update view with proposal ID
        view.proposal_id = proposal_id

    # === Information Commands ===

    @commands.command()
    @commands.guild_only()
    async def tree(self, ctx: commands.Context, user: discord.Member = None):
        """Display a visual family tree."""
        target = user or ctx.author

        if not self.visualizer.available:
            await ctx.send(
                "Family tree visualization is not available. "
                "The bot owner needs to install `networkx` and `matplotlib`."
            )
            return

        async with ctx.typing():
            image_buffer = await self.visualizer.generate_tree(
                self.db, target.id, self.bot, depth=2
            )

            if not image_buffer:
                await ctx.send(f"{target.display_name} has no family connections yet!")
                return

            file = discord.File(image_buffer, filename="family_tree.png")

            embed = discord.Embed(
                title=f"Family Tree for {target.display_name}",
                color=await ctx.embed_color()
            )
            embed.set_image(url="attachment://family_tree.png")

            await ctx.send(embed=embed, file=file)

    @commands.command()
    @commands.guild_only()
    @commands.admin_or_permissions(administrator=True)
    async def servertree(self, ctx: commands.Context):
        """Display a visual family tree for everyone in the server with relations."""
        if not self.visualizer.available:
            await ctx.send(
                "Family tree visualization is not available. "
                "The bot owner needs to install `Pillow`."
            )
            return

        async with ctx.typing():
            image_buffer = await self.visualizer.generate_server_tree(
                self.db, self.bot
            )

            if not image_buffer:
                await ctx.send("No family connections exist yet!")
                return

            file = discord.File(image_buffer, filename="server_family_tree.png")

            embed = discord.Embed(
                title="Server Family Tree",
                description="All family connections in this server",
                color=await ctx.embed_color()
            )
            embed.set_image(url="attachment://server_family_tree.png")

            await ctx.send(embed=embed, file=file)

    @commands.command()
    @commands.guild_only()
    async def family(self, ctx: commands.Context, user: discord.Member = None):
        """Display family information for a user."""
        target = user or ctx.author

        spouses = await self.db.get_spouses(target.id)
        parents = await self.db.get_parents(target.id)
        children = await self.db.get_children(target.id)
        siblings = await self.db.get_siblings(target.id)

        embed = discord.Embed(
            title=f"Family of {target.display_name}",
            color=await ctx.embed_color()
        )

        if spouses:
            spouse_names = []
            for s in spouses:
                user_obj = self.bot.get_user(s)
                spouse_names.append(user_obj.display_name if user_obj else f"User {s}")
            embed.add_field(
                name=f"\U0001f48d Spouse{'s' if len(spouses) > 1 else ''} ({len(spouses)})",
                value="\n".join(spouse_names),
                inline=True
            )

        if parents:
            parent_names = []
            for p in parents:
                user_obj = self.bot.get_user(p)
                parent_names.append(user_obj.display_name if user_obj else f"User {p}")
            embed.add_field(
                name=f"\U0001f9d1 Parent{'s' if len(parents) > 1 else ''} ({len(parents)})",
                value="\n".join(parent_names),
                inline=True
            )

        if children:
            child_names = []
            for c in children:
                user_obj = self.bot.get_user(c)
                child_names.append(user_obj.display_name if user_obj else f"User {c}")
            embed.add_field(
                name=f"\U0001f476 Child{'ren' if len(children) > 1 else ''} ({len(children)})",
                value="\n".join(child_names),
                inline=True
            )

        if siblings:
            sibling_names = []
            for s in siblings:
                user_obj = self.bot.get_user(s)
                sibling_names.append(user_obj.display_name if user_obj else f"User {s}")
            embed.add_field(
                name=f"\U0001f9d1\u200d\U0001f91d\u200d\U0001f9d1 Sibling{'s' if len(siblings) > 1 else ''} ({len(siblings)})",
                value="\n".join(sibling_names),
                inline=True
            )

        if not any([spouses, parents, children, siblings]):
            embed.description = "This user has no family connections yet."

        await ctx.send(embed=embed)

    @commands.command()
    @commands.guild_only()
    async def relationship(self, ctx: commands.Context, user1: discord.Member, user2: discord.Member):
        """Check the relationship between two users."""
        if user1.id == user2.id:
            await ctx.send("That's the same person!")
            return

        rel_type = await self.db.get_relationship_type(user1.id, user2.id)

        if rel_type:
            await ctx.send(
                f"**{user2.display_name}** is **{user1.display_name}**'s {rel_type}."
            )
        elif await self.db.are_related(user1.id, user2.id):
            await ctx.send(
                f"**{user1.display_name}** and **{user2.display_name}** are related (extended family)."
            )
        else:
            await ctx.send(
                f"**{user1.display_name}** and **{user2.display_name}** are not related."
            )

    @commands.command()
    @commands.guild_only()
    async def proposals(self, ctx: commands.Context):
        """View your pending proposals."""
        pending = await self.db.get_pending_proposals_for_user(ctx.author.id)

        if not pending:
            await ctx.send("You have no pending proposals.")
            return

        embed = discord.Embed(
            title="Your Pending Proposals",
            color=await ctx.embed_color()
        )

        for p in pending:
            proposer = self.bot.get_user(p["proposer_id"])
            proposer_name = proposer.display_name if proposer else f"User {p['proposer_id']}"

            type_emoji = {
                "marriage": "\U0001f48d",
                "adoption": "\U0001f476",
                "sire": "\U0001f46a"
            }.get(p["proposal_type"], "\u2753")

            embed.add_field(
                name=f"{type_emoji} {p['proposal_type'].title()} from {proposer_name}",
                value=f"[Jump to message](https://discord.com/channels/{p['guild_id']}/{p['channel_id']}/{p['message_id']})",
                inline=False
            )

        await ctx.send(embed=embed)

    # === Family Profile Commands ===

    @commands.group(invoke_without_command=True)
    @commands.guild_only()
    async def familyprofile(self, ctx: commands.Context, user: discord.Member = None):
        """View or manage your family profile (title, motto, crest)."""
        target = user or ctx.author
        profile = await self.db.get_family_profile(target.id)

        embed = discord.Embed(
            title=f"Family Profile: {target.display_name}",
            color=await ctx.embed_color()
        )

        if profile:
            if profile.get("family_title"):
                embed.add_field(name="Family Title", value=profile["family_title"], inline=False)
            if profile.get("family_motto"):
                embed.add_field(name="Family Motto", value=f"*\"{profile['family_motto']}\"*", inline=False)
            if profile.get("family_crest_url"):
                embed.set_thumbnail(url=profile["family_crest_url"])
                embed.add_field(name="Family Crest", value="See thumbnail", inline=False)
            if profile.get("looking_for_match"):
                bio = profile.get("match_bio") or "No bio set"
                embed.add_field(name="Looking for Match", value=bio, inline=False)

        if not profile or not any([profile.get("family_title"), profile.get("family_motto"), profile.get("family_crest_url")]):
            embed.description = "No family profile set up yet."
            if target.id == ctx.author.id:
                embed.description += f"\n\nUse `{ctx.clean_prefix}familyprofile title <name>` to set a family title!"

        await ctx.send(embed=embed)

    @familyprofile.command(name="title")
    async def familyprofile_title(self, ctx: commands.Context, *, title: str = None):
        """Set your family title (surname, dynasty name, house name, etc.)."""
        if title and len(title) > 50:
            await ctx.send("Family title must be 50 characters or less!")
            return

        await self.db.set_family_title(ctx.author.id, title)

        if title:
            await ctx.send(f"Your family title has been set to: **{title}**")
        else:
            await ctx.send("Your family title has been cleared.")

    @familyprofile.command(name="motto")
    async def familyprofile_motto(self, ctx: commands.Context, *, motto: str = None):
        """Set your family motto."""
        if motto and len(motto) > 200:
            await ctx.send("Family motto must be 200 characters or less!")
            return

        await self.db.set_family_motto(ctx.author.id, motto)

        if motto:
            await ctx.send(f"Your family motto has been set to: *\"{motto}\"*")
        else:
            await ctx.send("Your family motto has been cleared.")

    @familyprofile.command(name="crest")
    async def familyprofile_crest(self, ctx: commands.Context, url: str = None):
        """Set your family crest/banner image URL. Attach an image or provide a URL."""
        # Check for attached image
        if ctx.message.attachments:
            attachment = ctx.message.attachments[0]
            if attachment.content_type and attachment.content_type.startswith("image/"):
                url = attachment.url
            else:
                await ctx.send("Please attach a valid image file!")
                return

        if url:
            # Basic URL validation
            if not url.startswith(("http://", "https://")):
                await ctx.send("Please provide a valid URL starting with http:// or https://")
                return

            await self.db.set_family_crest(ctx.author.id, url)
            embed = discord.Embed(
                title="Family Crest Set!",
                color=await ctx.embed_color()
            )
            embed.set_thumbnail(url=url)
            await ctx.send(embed=embed)
        else:
            await self.db.set_family_crest(ctx.author.id, None)
            await ctx.send("Your family crest has been cleared.")

    # === Matchmaking Commands ===

    def _calculate_compatibility(self, user1_id: int, user2_id: int) -> dict:
        """Calculate fake but deterministic compatibility stats between two users."""
        import hashlib

        # Create a deterministic seed from both user IDs (order-independent)
        combined = min(user1_id, user2_id) * max(user1_id, user2_id)
        seed = int(hashlib.md5(str(combined).encode()).hexdigest()[:8], 16)

        # Generate "stats" based on the seed
        def stat(offset: int) -> int:
            return ((seed + offset * 7919) % 61) + 40  # 40-100 range

        stats = {
            "emotional_sync": stat(1),
            "humor_compatibility": stat(2),
            "adventure_spirit": stat(3),
            "communication": stat(4),
            "trust_potential": stat(5),
            "chaos_alignment": stat(6),
            "vibe_match": stat(7),
            "destiny_score": stat(8),
        }

        # Overall compatibility is weighted average
        weights = [1.2, 1.0, 0.8, 1.1, 1.3, 0.7, 1.0, 1.5]
        total = sum(s * w for s, w in zip(stats.values(), weights))
        stats["overall"] = int(total / sum(weights))

        return stats

    def _get_compatibility_rating(self, score: int) -> str:
        """Get a fun rating based on compatibility score."""
        if score >= 95:
            return "Soulmates"
        elif score >= 85:
            return "Perfect Match"
        elif score >= 75:
            return "Highly Compatible"
        elif score >= 65:
            return "Good Potential"
        elif score >= 55:
            return "Worth a Shot"
        elif score >= 45:
            return "It's Complicated"
        else:
            return "Chaotic Energy"

    def _score_bar(self, score: int, length: int = 10) -> str:
        """Create a visual bar for a score."""
        filled = int((score / 100) * length)
        return "█" * filled + "░" * (length - filled)

    @commands.command()
    @commands.guild_only()
    async def matchmaking(self, ctx: commands.Context, user: discord.Member = None):
        """Find your most compatible match in the server!"""
        target = user or ctx.author

        # Check if already married (unless polyamory)
        spouses = await self.db.get_spouses(target.id)
        polyamory = await self.get_effective_setting(ctx.guild.id, "polyamory")
        if spouses and not polyamory:
            spouse_names = []
            for s in spouses:
                u = self.bot.get_user(s)
                spouse_names.append(u.display_name if u else f"User {s}")
            await ctx.send(f"{target.display_name} is already happily married to {', '.join(spouse_names)}!")
            return

        # Find all eligible singles in the guild
        candidates = []
        for member in ctx.guild.members:
            if member.bot or member.id == target.id:
                continue
            # Skip if they're married (unless polyamory)
            member_spouses = await self.db.get_spouses(member.id)
            if member_spouses and not polyamory:
                continue
            # Skip if already married to target
            if member.id in spouses:
                continue
            # Skip if related (incest check)
            incest = await self.get_effective_setting(ctx.guild.id, "incest")
            if not incest and await self.db.are_related(target.id, member.id):
                continue

            # Calculate compatibility
            stats = self._calculate_compatibility(target.id, member.id)
            candidates.append((member, stats))

        if not candidates:
            await ctx.send(f"No eligible matches found for {target.display_name}!")
            return

        # Sort by overall compatibility
        candidates.sort(key=lambda x: x[1]["overall"], reverse=True)

        # Get top 5 matches
        top_matches = candidates[:5]
        best_match, best_stats = top_matches[0]

        embed = discord.Embed(
            title=f"Matchmaking Results for {target.display_name}",
            color=discord.Color.pink()
        )

        # Show best match with full stats
        rating = self._get_compatibility_rating(best_stats["overall"])
        embed.add_field(
            name=f"Best Match: {best_match.display_name}",
            value=(
                f"**{rating}** - {best_stats['overall']}% Compatible\n\n"
                f"Emotional Sync: {self._score_bar(best_stats['emotional_sync'])} {best_stats['emotional_sync']}%\n"
                f"Humor Match: {self._score_bar(best_stats['humor_compatibility'])} {best_stats['humor_compatibility']}%\n"
                f"Adventure Spirit: {self._score_bar(best_stats['adventure_spirit'])} {best_stats['adventure_spirit']}%\n"
                f"Communication: {self._score_bar(best_stats['communication'])} {best_stats['communication']}%\n"
                f"Trust Potential: {self._score_bar(best_stats['trust_potential'])} {best_stats['trust_potential']}%\n"
                f"Chaos Alignment: {self._score_bar(best_stats['chaos_alignment'])} {best_stats['chaos_alignment']}%\n"
                f"Vibe Match: {self._score_bar(best_stats['vibe_match'])} {best_stats['vibe_match']}%\n"
                f"Destiny Score: {self._score_bar(best_stats['destiny_score'])} {best_stats['destiny_score']}%"
            ),
            inline=False
        )

        # Show other top matches briefly
        if len(top_matches) > 1:
            other_matches = []
            for member, stats in top_matches[1:]:
                rating = self._get_compatibility_rating(stats["overall"])
                other_matches.append(f"**{member.display_name}** - {stats['overall']}% ({rating})")

            embed.add_field(
                name="Other Potential Matches",
                value="\n".join(other_matches),
                inline=False
            )

        embed.set_footer(text=f"Use {ctx.clean_prefix}marry @user to shoot your shot!")
        embed.set_thumbnail(url=best_match.display_avatar.url)

        await ctx.send(embed=embed)

    @commands.command()
    @commands.guild_only()
    async def compatibility(self, ctx: commands.Context, user1: discord.Member, user2: discord.Member = None):
        """Check compatibility between two users."""
        if user2 is None:
            user2 = user1
            user1 = ctx.author

        if user1.id == user2.id:
            await ctx.send("Self-love is important, but that's not how this works!")
            return

        stats = self._calculate_compatibility(user1.id, user2.id)
        rating = self._get_compatibility_rating(stats["overall"])

        embed = discord.Embed(
            title=f"Compatibility: {user1.display_name} & {user2.display_name}",
            description=f"**{rating}** - {stats['overall']}% Overall Compatibility",
            color=discord.Color.pink()
        )

        embed.add_field(
            name="Compatibility Breakdown",
            value=(
                f"Emotional Sync: {self._score_bar(stats['emotional_sync'])} {stats['emotional_sync']}%\n"
                f"Humor Match: {self._score_bar(stats['humor_compatibility'])} {stats['humor_compatibility']}%\n"
                f"Adventure Spirit: {self._score_bar(stats['adventure_spirit'])} {stats['adventure_spirit']}%\n"
                f"Communication: {self._score_bar(stats['communication'])} {stats['communication']}%\n"
                f"Trust Potential: {self._score_bar(stats['trust_potential'])} {stats['trust_potential']}%\n"
                f"Chaos Alignment: {self._score_bar(stats['chaos_alignment'])} {stats['chaos_alignment']}%\n"
                f"Vibe Match: {self._score_bar(stats['vibe_match'])} {stats['vibe_match']}%\n"
                f"Destiny Score: {self._score_bar(stats['destiny_score'])} {stats['destiny_score']}%"
            ),
            inline=False
        )

        # Fun commentary based on score
        if stats["overall"] >= 90:
            comment = "The stars have aligned! This is meant to be."
        elif stats["overall"] >= 75:
            comment = "Strong potential here. The universe approves."
        elif stats["overall"] >= 60:
            comment = "Could work with some effort. Love finds a way!"
        elif stats["overall"] >= 45:
            comment = "Opposites attract... sometimes. Proceed with caution."
        else:
            comment = "Chaotic energy detected. This could be entertaining."

        embed.set_footer(text=comment)
        await ctx.send(embed=embed)

    @commands.command(name="familyhelp")
    @commands.guild_only()
    async def familyhelp(self, ctx: commands.Context):
        """Show all family commands."""
        prefix = ctx.clean_prefix

        embed = discord.Embed(
            title="\U0001f46a Family Commands",
            description="Create and manage your virtual family!",
            color=await ctx.embed_color()
        )

        # Relationship commands
        relationship_cmds = (
            f"`{prefix}marry @user` - Propose marriage\n"
            f"`{prefix}divorce @user` - Divorce your spouse\n"
            f"`{prefix}adopt @user` - Adopt someone as your child\n"
            f"`{prefix}disown @user` - Disown your child\n"
            f"`{prefix}runaway` - Run away from a parent\n"
            f"`{prefix}sire @coparent @child` - Add a co-parent to your child"
        )
        embed.add_field(
            name="\U0001f48d Relationships",
            value=relationship_cmds,
            inline=False
        )

        # Info commands
        info_cmds = (
            f"`{prefix}family [@user]` - View family members\n"
            f"`{prefix}tree [@user]` - View family tree image\n"
            f"`{prefix}relationship @user1 @user2` - Check relationship\n"
            f"`{prefix}proposals` - View pending proposals"
        )
        embed.add_field(
            name="\U0001f4cb Information",
            value=info_cmds,
            inline=False
        )

        # Profile commands
        profile_cmds = (
            f"`{prefix}familyprofile [@user]` - View family profile\n"
            f"`{prefix}familyprofile title <name>` - Set family title/dynasty\n"
            f"`{prefix}familyprofile motto <text>` - Set family motto\n"
            f"`{prefix}familyprofile crest [url]` - Set family crest image"
        )
        embed.add_field(
            name="\U0001f3f0 Profile",
            value=profile_cmds,
            inline=False
        )

        # Matchmaking commands
        matchmaking_cmds = (
            f"`{prefix}matchmaking [@user]` - Find your best match in the server\n"
            f"`{prefix}compatibility @user` - Check compatibility with someone"
        )
        embed.add_field(
            name="\U0001f495 Matchmaking",
            value=matchmaking_cmds,
            inline=False
        )

        # Settings commands
        settings_cmds = (
            f"`{prefix}familyset polyamory [on/off]` - Toggle multiple marriages\n"
            f"`{prefix}familyset incest [on/off]` - Toggle family marriages\n"
            f"`{prefix}familyset timeout [seconds]` - Set proposal timeout\n"
            f"`{prefix}familyset settings` - View current settings"
        )
        embed.add_field(
            name="\u2699\ufe0f Settings (Admin)",
            value=settings_cmds,
            inline=False
        )

        embed.set_footer(text="Proposals require the other person to accept!")

        await ctx.send(embed=embed)

    # === Settings Commands ===

    @commands.group()
    @commands.guild_only()
    @checks.admin_or_permissions(manage_guild=True)
    async def familyset(self, ctx: commands.Context):
        """Configure family cog settings for this server."""
        if ctx.invoked_subcommand is None:
            await ctx.send_help(ctx.command)

    @familyset.command(name="polyamory")
    async def familyset_polyamory(self, ctx: commands.Context, enabled: bool = None):
        """Enable or disable polyamory (multiple marriages) for this server."""
        if enabled is None:
            current = await self.get_effective_setting(ctx.guild.id, "polyamory")
            status = "enabled" if current else "disabled"
            await ctx.send(f"Polyamory is currently **{status}** for this server.")
        else:
            await self.config.guild(ctx.guild).override_polyamory.set(enabled)
            status = "enabled" if enabled else "disabled"
            await ctx.send(f"Polyamory has been **{status}** for this server.")

    @familyset.command(name="incest")
    async def familyset_incest(self, ctx: commands.Context, enabled: bool = None):
        """Enable or disable incest (marriage between family members) for this server."""
        if enabled is None:
            current = await self.get_effective_setting(ctx.guild.id, "incest")
            status = "enabled" if current else "disabled"
            await ctx.send(f"Incest is currently **{status}** for this server.")
        else:
            await self.config.guild(ctx.guild).override_incest.set(enabled)
            status = "enabled" if enabled else "disabled"
            await ctx.send(f"Incest has been **{status}** for this server.")

    @familyset.command(name="timeout")
    async def familyset_timeout(self, ctx: commands.Context, seconds: int = None):
        """Set the proposal timeout in seconds (30-3600)."""
        if seconds is None:
            current = await self.get_effective_setting(ctx.guild.id, "proposal_timeout")
            await ctx.send(f"Proposal timeout is currently **{current} seconds** ({current // 60} minutes).")
        else:
            if seconds < 30 or seconds > 3600:
                await ctx.send("Timeout must be between 30 and 3600 seconds.")
                return
            await self.config.guild(ctx.guild).override_proposal_timeout.set(seconds)
            await ctx.send(f"Proposal timeout set to **{seconds} seconds** ({seconds // 60} minutes).")

    @familyset.command(name="settings")
    async def familyset_settings(self, ctx: commands.Context):
        """Display current family settings for this server."""
        polyamory = await self.get_effective_setting(ctx.guild.id, "polyamory")
        incest = await self.get_effective_setting(ctx.guild.id, "incest")
        timeout = await self.get_effective_setting(ctx.guild.id, "proposal_timeout")
        max_spouses = await self.config.max_spouses()
        max_children = await self.config.max_children()

        embed = discord.Embed(
            title="Family Settings",
            color=await ctx.embed_color()
        )
        embed.add_field(name="Polyamory", value="Enabled" if polyamory else "Disabled", inline=True)
        embed.add_field(name="Incest", value="Enabled" if incest else "Disabled", inline=True)
        embed.add_field(name="Proposal Timeout", value=f"{timeout}s ({timeout // 60}m)", inline=True)
        embed.add_field(name="Max Spouses", value=str(max_spouses), inline=True)
        embed.add_field(name="Max Children", value=str(max_children), inline=True)

        await ctx.send(embed=embed)

    @familyset.command(name="reset")
    async def familyset_reset(self, ctx: commands.Context):
        """Reset all server settings to use global defaults."""
        await self.config.guild(ctx.guild).override_polyamory.set(None)
        await self.config.guild(ctx.guild).override_incest.set(None)
        await self.config.guild(ctx.guild).override_proposal_timeout.set(None)
        await ctx.send("All server settings have been reset to global defaults.")

    # === Global Owner Settings ===

    @commands.group()
    @commands.is_owner()
    async def familysetglobal(self, ctx: commands.Context):
        """Configure global family settings (bot owner only)."""
        if ctx.invoked_subcommand is None:
            await ctx.send_help(ctx.command)

    @familysetglobal.command(name="polyamory")
    async def familysetglobal_polyamory(self, ctx: commands.Context, enabled: bool):
        """Set the global default for polyamory."""
        await self.config.polyamory_enabled.set(enabled)
        status = "enabled" if enabled else "disabled"
        await ctx.send(f"Global polyamory default set to **{status}**.")

    @familysetglobal.command(name="incest")
    async def familysetglobal_incest(self, ctx: commands.Context, enabled: bool):
        """Set the global default for incest."""
        await self.config.incest_enabled.set(enabled)
        status = "enabled" if enabled else "disabled"
        await ctx.send(f"Global incest default set to **{status}**.")

    @familysetglobal.command(name="timeout")
    async def familysetglobal_timeout(self, ctx: commands.Context, seconds: int):
        """Set the global default proposal timeout."""
        if seconds < 30 or seconds > 3600:
            await ctx.send("Timeout must be between 30 and 3600 seconds.")
            return
        await self.config.proposal_timeout.set(seconds)
        await ctx.send(f"Global proposal timeout set to **{seconds} seconds**.")

    @familysetglobal.command(name="maxspouses")
    async def familysetglobal_maxspouses(self, ctx: commands.Context, count: int):
        """Set the maximum number of spouses (when polyamory is enabled)."""
        if count < 1 or count > 20:
            await ctx.send("Max spouses must be between 1 and 20.")
            return
        await self.config.max_spouses.set(count)
        await ctx.send(f"Maximum spouses set to **{count}**.")

    @familysetglobal.command(name="maxchildren")
    async def familysetglobal_maxchildren(self, ctx: commands.Context, count: int):
        """Set the maximum number of children per user."""
        if count < 1 or count > 50:
            await ctx.send("Max children must be between 1 and 50.")
            return
        await self.config.max_children.set(count)
        await ctx.send(f"Maximum children set to **{count}**.")

    @familysetglobal.command(name="settings")
    async def familysetglobal_settings(self, ctx: commands.Context):
        """Display current global family settings."""
        polyamory = await self.config.polyamory_enabled()
        incest = await self.config.incest_enabled()
        timeout = await self.config.proposal_timeout()
        max_spouses = await self.config.max_spouses()
        max_children = await self.config.max_children()

        embed = discord.Embed(
            title="Global Family Settings",
            color=await ctx.embed_color()
        )
        embed.add_field(name="Polyamory Default", value="Enabled" if polyamory else "Disabled", inline=True)
        embed.add_field(name="Incest Default", value="Enabled" if incest else "Disabled", inline=True)
        embed.add_field(name="Proposal Timeout", value=f"{timeout}s ({timeout // 60}m)", inline=True)
        embed.add_field(name="Max Spouses", value=str(max_spouses), inline=True)
        embed.add_field(name="Max Children", value=str(max_children), inline=True)

        await ctx.send(embed=embed)

    @familysetglobal.command(name="resetall")
    async def familysetglobal_resetall(self, ctx: commands.Context, confirm: str = None):
        """Reset ALL family data globally. Use with 'confirm' to execute."""
        if confirm != "confirm":
            await ctx.send(
                "⚠️ **WARNING**: This will delete ALL family data (marriages, adoptions, proposals) globally!\n\n"
                f"To confirm, run: `{ctx.prefix}familysetglobal resetall confirm`"
            )
            return

        await self.db.reset_all()
        await ctx.send("✅ All family data has been reset.")