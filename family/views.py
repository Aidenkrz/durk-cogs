import discord
from discord.ui import View, Button, Select
from typing import TYPE_CHECKING, Optional, Set
import logging

if TYPE_CHECKING:
    from .family import Family

log = logging.getLogger("red.DurkCogs.Family.views")


class ProposalView(View):
    """View for marriage and adoption proposals with Accept/Decline buttons."""

    def __init__(
        self,
        cog: "Family",
        proposal_id: int,
        target_id: int,
        proposal_type: str,
        timeout: float = 300.0
    ):
        super().__init__(timeout=timeout)
        self.cog = cog
        self.proposal_id = proposal_id
        self.target_id = target_id
        self.proposal_type = proposal_type
        self.responded = False

    @discord.ui.button(label="Accept", style=discord.ButtonStyle.green, emoji="\u2764\ufe0f")
    async def accept_button(self, interaction: discord.Interaction, button: Button):
        if interaction.user.id != self.target_id:
            await interaction.response.send_message(
                "Only the person being proposed to can respond!",
                ephemeral=True
            )
            return

        self.responded = True
        self.stop()

        if self.proposal_type == "marriage":
            await self.cog.handle_marriage_accept(interaction, self.proposal_id)
        elif self.proposal_type == "adoption":
            await self.cog.handle_adoption_accept(interaction, self.proposal_id)

    @discord.ui.button(label="Decline", style=discord.ButtonStyle.red, emoji="\U0001f494")
    async def decline_button(self, interaction: discord.Interaction, button: Button):
        if interaction.user.id != self.target_id:
            await interaction.response.send_message(
                "Only the person being proposed to can respond!",
                ephemeral=True
            )
            return

        self.responded = True
        self.stop()
        await self.cog.handle_proposal_decline(interaction, self.proposal_id, self.proposal_type)

    async def on_timeout(self):
        if not self.responded:
            try:
                await self.cog.handle_proposal_timeout(self.proposal_id)
            except Exception:
                pass


class SireProposalView(View):
    """
    View for sire proposals requiring both co-parent and child to accept.
    Parent A initiates, Parent B (co-parent) and Child must both accept.
    """

    def __init__(
        self,
        cog: "Family",
        proposal_id: int,
        coparent_id: int,
        child_id: int,
        timeout: float = 300.0
    ):
        super().__init__(timeout=timeout)
        self.cog = cog
        self.proposal_id = proposal_id
        self.coparent_id = coparent_id
        self.child_id = child_id
        self.coparent_accepted = False
        self.child_accepted = False
        self.responded = False

    @discord.ui.button(label="Accept", style=discord.ButtonStyle.green, emoji="\u2705")
    async def accept_button(self, interaction: discord.Interaction, button: Button):
        user_id = interaction.user.id

        if user_id not in (self.coparent_id, self.child_id):
            await interaction.response.send_message(
                "Only the co-parent or child can respond to this proposal!",
                ephemeral=True
            )
            return

        if user_id == self.coparent_id:
            if self.coparent_accepted:
                await interaction.response.send_message(
                    "You've already accepted!",
                    ephemeral=True
                )
                return
            self.coparent_accepted = True
        else:
            if self.child_accepted:
                await interaction.response.send_message(
                    "You've already accepted!",
                    ephemeral=True
                )
                return
            self.child_accepted = True

        if self.coparent_accepted and self.child_accepted:
            self.responded = True
            self.stop()
            await self.cog.handle_sire_complete(interaction, self.proposal_id)
        else:
            waiting_for = "the child" if self.coparent_accepted else "the co-parent"
            await interaction.response.send_message(
                f"You've accepted! Waiting for {waiting_for} to also accept...",
                ephemeral=True
            )
            # Update the embed to show partial acceptance
            await self._update_embed(interaction)

    @discord.ui.button(label="Decline", style=discord.ButtonStyle.red, emoji="\u274c")
    async def decline_button(self, interaction: discord.Interaction, button: Button):
        user_id = interaction.user.id

        if user_id not in (self.coparent_id, self.child_id):
            await interaction.response.send_message(
                "Only the co-parent or child can respond to this proposal!",
                ephemeral=True
            )
            return

        self.responded = True
        self.stop()
        await self.cog.handle_proposal_decline(interaction, self.proposal_id, "sire")

    async def _update_embed(self, interaction: discord.Interaction):
        """Update the embed to show who has accepted."""
        embed = interaction.message.embeds[0] if interaction.message.embeds else None
        if embed:
            status_lines = []
            coparent = interaction.guild.get_member(self.coparent_id) if interaction.guild else None
            child = interaction.guild.get_member(self.child_id) if interaction.guild else None

            coparent_name = coparent.display_name if coparent else f"User {self.coparent_id}"
            child_name = child.display_name if child else f"User {self.child_id}"

            coparent_status = "\u2705" if self.coparent_accepted else "\u23f3"
            child_status = "\u2705" if self.child_accepted else "\u23f3"

            embed.set_field_at(
                0,
                name="Acceptance Status",
                value=f"{coparent_status} {coparent_name} (Co-parent)\n{child_status} {child_name} (Child)",
                inline=False
            )
            await interaction.message.edit(embed=embed)

    async def on_timeout(self):
        if not self.responded:
            try:
                await self.cog.handle_proposal_timeout(self.proposal_id)
            except Exception:
                # Cog may have been unloaded or database closed
                pass


class RunawaySelectView(View):
    """View for selecting which parent to run away from."""

    def __init__(
        self,
        cog: "Family",
        child_id: int,
        parents: list,
        timeout: float = 60.0
    ):
        super().__init__(timeout=timeout)
        self.cog = cog
        self.child_id = child_id
        self.responded = False

        options = [
            discord.SelectOption(
                label=parent["name"][:100],
                value=str(parent["id"]),
                description=f"Run away from {parent['name'][:50]}"
            )
            for parent in parents
        ]

        self.select = Select(
            placeholder="Choose a parent to run away from...",
            options=options,
            min_values=1,
            max_values=1
        )
        self.select.callback = self.select_callback
        self.add_item(self.select)

    async def select_callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.child_id:
            await interaction.response.send_message(
                "This isn't your choice to make!",
                ephemeral=True
            )
            return

        self.responded = True
        self.stop()
        parent_id = int(self.select.values[0])
        await self.cog.execute_runaway(interaction, self.child_id, parent_id)

    async def on_timeout(self):
        pass


class PersistentProposalView(View):
    """
    Persistent view for proposals that survives bot restarts.
    Uses custom_id patterns to identify the proposal.
    """

    def __init__(self, cog: "Family"):
        super().__init__(timeout=None)
        self.cog = cog

    @discord.ui.button(
        label="Accept",
        style=discord.ButtonStyle.green,
        emoji="\u2764\ufe0f",
        custom_id="family:proposal:accept"
    )
    async def accept_button(self, interaction: discord.Interaction, button: Button):
        await self._handle_button(interaction, accepted=True)

    @discord.ui.button(
        label="Decline",
        style=discord.ButtonStyle.red,
        emoji="\U0001f494",
        custom_id="family:proposal:decline"
    )
    async def decline_button(self, interaction: discord.Interaction, button: Button):
        await self._handle_button(interaction, accepted=False)

    async def _handle_button(self, interaction: discord.Interaction, accepted: bool):
        # Get proposal from database using message ID
        proposal = await self.cog.db.get_proposal_by_message(interaction.message.id)
        if not proposal:
            await interaction.response.send_message(
                "This proposal has expired or no longer exists.",
                ephemeral=True
            )
            return

        # Check if user is authorized to respond
        if interaction.user.id != proposal["target_id"]:
            await interaction.response.send_message(
                "Only the person being proposed to can respond!",
                ephemeral=True
            )
            return

        if accepted:
            if proposal["proposal_type"] == "marriage":
                await self.cog.handle_marriage_accept(interaction, proposal["id"])
            elif proposal["proposal_type"] == "adoption":
                await self.cog.handle_adoption_accept(interaction, proposal["id"])
        else:
            await self.cog.handle_proposal_decline(
                interaction, proposal["id"], proposal["proposal_type"]
            )


class PersistentSireView(View):
    """
    Persistent view for sire proposals that survives bot restarts.
    Tracks acceptance state in memory (will reset on restart).
    """

    # Class-level tracking of partial acceptances
    _acceptance_state: dict = {}

    def __init__(self, cog: "Family"):
        super().__init__(timeout=None)
        self.cog = cog

    @discord.ui.button(
        label="Accept",
        style=discord.ButtonStyle.green,
        emoji="\u2705",
        custom_id="family:sire:accept"
    )
    async def accept_button(self, interaction: discord.Interaction, button: Button):
        proposal = await self.cog.db.get_proposal_by_message(interaction.message.id)
        if not proposal:
            await interaction.response.send_message(
                "This proposal has expired or no longer exists.",
                ephemeral=True
            )
            return

        user_id = interaction.user.id
        coparent_id = proposal["target_id"]
        child_id = proposal["child_id"]

        if user_id not in (coparent_id, child_id):
            await interaction.response.send_message(
                "Only the co-parent or child can respond to this proposal!",
                ephemeral=True
            )
            return

        # Track acceptance state
        msg_id = interaction.message.id
        if msg_id not in self._acceptance_state:
            self._acceptance_state[msg_id] = {"coparent": False, "child": False}

        state = self._acceptance_state[msg_id]

        if user_id == coparent_id:
            if state["coparent"]:
                await interaction.response.send_message("You've already accepted!", ephemeral=True)
                return
            state["coparent"] = True
        else:
            if state["child"]:
                await interaction.response.send_message("You've already accepted!", ephemeral=True)
                return
            state["child"] = True

        if state["coparent"] and state["child"]:
            del self._acceptance_state[msg_id]
            await self.cog.handle_sire_complete(interaction, proposal["id"])
        else:
            waiting_for = "the child" if state["coparent"] else "the co-parent"
            await interaction.response.send_message(
                f"You've accepted! Waiting for {waiting_for} to also accept...",
                ephemeral=True
            )

    @discord.ui.button(
        label="Decline",
        style=discord.ButtonStyle.red,
        emoji="\u274c",
        custom_id="family:sire:decline"
    )
    async def decline_button(self, interaction: discord.Interaction, button: Button):
        proposal = await self.cog.db.get_proposal_by_message(interaction.message.id)
        if not proposal:
            await interaction.response.send_message(
                "This proposal has expired or no longer exists.",
                ephemeral=True
            )
            return

        user_id = interaction.user.id
        coparent_id = proposal["target_id"]
        child_id = proposal["child_id"]

        if user_id not in (coparent_id, child_id):
            await interaction.response.send_message(
                "Only the co-parent or child can respond to this proposal!",
                ephemeral=True
            )
            return

        # Clean up state
        msg_id = interaction.message.id
        if msg_id in self._acceptance_state:
            del self._acceptance_state[msg_id]

        await self.cog.handle_proposal_decline(interaction, proposal["id"], "sire")
