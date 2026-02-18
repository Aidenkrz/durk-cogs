import discord
from redbot.core import commands, Config, checks
from redbot.core.bot import Red
import logging
from typing import Optional

log = logging.getLogger("red.durk-cogs.rolegiver")


class RoleGiver(commands.Cog):
    """Allow specific roles to grant other roles via custom commands."""

    def __init__(self, bot: Red):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=928374650192)
        default_guild = {
            "commands": {}
        }
        self.config.register_guild(**default_guild)

    @commands.group()
    @checks.admin_or_permissions(administrator=True)
    async def rolegiver(self, ctx: commands.Context):
        """Manage role delegation commands."""
        pass

    @rolegiver.command(name="add")
    async def rolegiver_add(
        self,
        ctx: commands.Context,
        giver_role: discord.Role,
        target_role: discord.Role,
        command_name: str
    ):
        """Allow a role to give another role via a custom command.

        Example: `[p]rolegiver add @Maintainer @Contributor contrib`
        Then users with Maintainer can run `[p]contrib @user` to give Contributor.
        """
        command_name = command_name.lower().strip()

        if not command_name.isalnum():
            embed = discord.Embed(
                title="Error",
                description="Command name must be alphanumeric only.",
                color=discord.Color.red()
            )
            await ctx.send(embed=embed)
            return

        if self.bot.get_command(command_name):
            embed = discord.Embed(
                title="Error",
                description=f"A bot command `{command_name}` already exists. Choose a different name.",
                color=discord.Color.red()
            )
            await ctx.send(embed=embed)
            return

        bot_top_role = ctx.guild.me.top_role
        if target_role >= bot_top_role:
            embed = discord.Embed(
                title="Error",
                description=f"I cannot assign {target_role.mention} because it is at or above my highest role.",
                color=discord.Color.red()
            )
            await ctx.send(embed=embed)
            return

        async with self.config.guild(ctx.guild).commands() as cmds:
            cmds[command_name] = {
                "giver_role": giver_role.id,
                "target_role": target_role.id
            }

        embed = discord.Embed(
            title="Role Command Created",
            description=(
                f"Users with {giver_role.mention} can now use `{ctx.prefix}{command_name} @user` "
                f"to give {target_role.mention}."
            ),
            color=discord.Color.green()
        )
        await ctx.send(embed=embed)

    @rolegiver.command(name="remove")
    async def rolegiver_remove(self, ctx: commands.Context, command_name: str):
        """Remove a role delegation command."""
        command_name = command_name.lower().strip()

        async with self.config.guild(ctx.guild).commands() as cmds:
            if command_name not in cmds:
                embed = discord.Embed(
                    title="Error",
                    description=f"Command `{command_name}` not found.",
                    color=discord.Color.red()
                )
                await ctx.send(embed=embed)
                return
            del cmds[command_name]

        embed = discord.Embed(
            title="Role Command Removed",
            description=f"Command `{command_name}` has been removed.",
            color=discord.Color.green()
        )
        await ctx.send(embed=embed)

    @rolegiver.command(name="list")
    async def rolegiver_list(self, ctx: commands.Context):
        """List all configured role delegation commands."""
        cmds = await self.config.guild(ctx.guild).commands()

        if not cmds:
            embed = discord.Embed(
                title="Role Delegation Commands",
                description="No commands configured.",
                color=discord.Color.blue()
            )
            await ctx.send(embed=embed)
            return

        embed = discord.Embed(
            title="Role Delegation Commands",
            color=discord.Color.blue()
        )

        for cmd_name, data in cmds.items():
            giver_role = ctx.guild.get_role(data["giver_role"])
            target_role = ctx.guild.get_role(data["target_role"])

            giver_str = giver_role.mention if giver_role else f"Deleted Role ({data['giver_role']})"
            target_str = target_role.mention if target_role else f"Deleted Role ({data['target_role']})"

            embed.add_field(
                name=f"`{ctx.prefix}{cmd_name}`",
                value=f"**Giver:** {giver_str}\n**Target:** {target_str}",
                inline=False
            )

        await ctx.send(embed=embed)

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        """Listen for custom role commands."""
        if message.author.bot or not message.guild:
            return

        ctx = await self.bot.get_context(message)
        if ctx.valid:
            return

        prefixes = await self.bot.get_prefix(message)
        if isinstance(prefixes, str):
            prefixes = [prefixes]

        content = message.content
        used_prefix = None
        for prefix in prefixes:
            if content.startswith(prefix):
                used_prefix = prefix
                break

        if not used_prefix:
            return

        content_after_prefix = content[len(used_prefix):].strip()
        parts = content_after_prefix.split(maxsplit=1)
        if not parts:
            return

        command_name = parts[0].lower()
        args = parts[1] if len(parts) > 1 else ""

        cmds = await self.config.guild(message.guild).commands()
        if command_name not in cmds:
            return

        cmd_data = cmds[command_name]
        giver_role_id = cmd_data["giver_role"]
        target_role_id = cmd_data["target_role"]

        member = message.author
        if not any(r.id == giver_role_id for r in member.roles):
            embed = discord.Embed(
                title="Permission Denied",
                description="You don't have the required role to use this command.",
                color=discord.Color.red()
            )
            await message.channel.send(embed=embed)
            return

        target_role = message.guild.get_role(target_role_id)
        if not target_role:
            embed = discord.Embed(
                title="Error",
                description="The target role no longer exists.",
                color=discord.Color.red()
            )
            await message.channel.send(embed=embed)
            return

        if not message.mentions:
            embed = discord.Embed(
                title="Usage",
                description=f"`{used_prefix}{command_name} @user` - Give {target_role.mention} to a user.",
                color=discord.Color.blue()
            )
            await message.channel.send(embed=embed)
            return

        target_member = message.mentions[0]

        if target_role in target_member.roles:
            embed = discord.Embed(
                title="Already Has Role",
                description=f"{target_member.mention} already has {target_role.mention}.",
                color=discord.Color.orange()
            )
            await message.channel.send(embed=embed)
            return

        bot_top_role = message.guild.me.top_role
        if target_role >= bot_top_role:
            embed = discord.Embed(
                title="Error",
                description=f"I cannot assign {target_role.mention} because it is at or above my highest role.",
                color=discord.Color.red()
            )
            await message.channel.send(embed=embed)
            return

        try:
            await target_member.add_roles(target_role, reason=f"RoleGiver: {member} used {command_name}")
            embed = discord.Embed(
                title="Role Assigned",
                description=f"{target_member.mention} has been given {target_role.mention}.",
                color=discord.Color.green()
            )
            await message.channel.send(embed=embed)
        except discord.Forbidden:
            embed = discord.Embed(
                title="Error",
                description="I don't have permission to assign that role.",
                color=discord.Color.red()
            )
            await message.channel.send(embed=embed)
        except discord.HTTPException as e:
            log.error(f"Failed to assign role: {e}")
            embed = discord.Embed(
                title="Error",
                description="An error occurred while assigning the role.",
                color=discord.Color.red()
            )
            await message.channel.send(embed=embed)
