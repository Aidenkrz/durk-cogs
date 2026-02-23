import discord
from redbot.core import commands, Config, checks
from redbot.core.bot import Red
import logging
from typing import Set, Optional, Dict, Any

log = logging.getLogger("red.durk-cogs.rolesyncer")

SyncGroups = Dict[str, Dict[str, Any]]

class RoleSyncer(commands.Cog):
    """Cog for syncing specific roles unidirectionally from a master server to slave servers."""

    def __init__(self, bot: Red):
        self.bot = bot
        self.config = Config.get_conf(self, identifier="rolesync")
        default_global = {
            "sync_groups": {},
            "enabled": True
        }
        self.config.register_global(**default_global)

    @commands.group()
    @commands.is_owner()
    async def rolesync(self, ctx: commands.Context):
        """Manage master-slave role synchronization settings. (Bot owner only)"""
        pass

    @rolesync.command(name="create")
    async def rolesync_create(self, ctx: commands.Context, group_name: str):
        """Creates a new role synchronization group."""
        async with self.config.sync_groups() as groups:
            if group_name in groups:
                embed = discord.Embed(title="Error", description=f"Group '{group_name}' already exists.", color=discord.Color.red())
                await ctx.send(embed=embed)
                return
            groups[group_name] = {"master": None, "slaves": [], "roles": []}
        embed = discord.Embed(title="Sync Group Created", description=f"Sync group '{group_name}' created.\nUse `{ctx.prefix}rolesync setmaster`, `{ctx.prefix}rolesync addslave`, and `{ctx.prefix}rolesync addrole` to configure it.", color=discord.Color.green())
        await ctx.send(embed=embed)

    @rolesync.command(name="delete")
    async def rolesync_delete(self, ctx: commands.Context, group_name: str):
        """Deletes a role synchronization group."""
        async with self.config.sync_groups() as groups:
            if group_name not in groups:
                embed = discord.Embed(title="Error", description=f"Group '{group_name}' not found.", color=discord.Color.red())
                await ctx.send(embed=embed)
                return
            del groups[group_name]
        embed = discord.Embed(title="Sync Group Deleted", description=f"Sync group '{group_name}' deleted.", color=discord.Color.green())
        await ctx.send(embed=embed)

    @rolesync.command(name="list")
    async def rolesync_list(self, ctx: commands.Context):
        """Lists all role synchronization groups, their master/slaves, and synced roles."""
        groups: SyncGroups = await self.config.sync_groups()
        if not groups:
            embed = discord.Embed(title="Role Synchronization Groups", description="No sync groups configured.", color=discord.Color.blue())
            await ctx.send(embed=embed)
            return

        embed = discord.Embed(title="Role Synchronization Groups", color=discord.Color.blue())

        if len(groups) == 0:
             embed.description = "No sync groups configured."
             await ctx.send(embed=embed)
             return

        for name, data in groups.items():
            master_id = data.get("master")
            master_guild = self.bot.get_guild(master_id) if master_id else None
            master_str = f"{master_guild.name} (`{master_id}`)" if master_guild else (f"ID: `{master_id}`" if master_id else "*Not Set*")

            slaves_list = []
            for slave_id in data.get("slaves", []):
                 slave_guild = self.bot.get_guild(slave_id)
                 slaves_list.append(f"{slave_guild.name} (`{slave_id}`)" if slave_guild else f"ID: `{slave_id}`")

            slaves_str = "\n".join(slaves_list) if len(slaves_list) > 1 else (slaves_list[0] if slaves_list else "*None*")


            role_list = data.get("roles", [])

            roles_str = "\n".join(f"- `{r}`" for r in role_list) if role_list else "*No roles configured*"

            embed.add_field(
                name=f"🔄 Group: {name}",
                value=(
                    f"👑 **Master:** {master_str}\n"
                    f"🔗 **Slaves:**\n{slaves_str}\n"
                    f"📜 **Synced Roles:**\n{roles_str}"
                ),
                inline=False
            )

            if len(groups) > 1 and name != list(groups.keys())[-1]:
                 embed.add_field(name="\u200b", value="\u200b", inline=False)


        await ctx.send(embed=embed)

    @rolesync.command(name="setmaster")
    async def rolesync_setmaster(self, ctx: commands.Context, group_name: str, guild_id: int):
        """Sets the master server (by ID) for a sync group."""
        guild = self.bot.get_guild(guild_id)
        if not guild:
            embed = discord.Embed(title="Error", description=f"Could not find a server with ID `{guild_id}`. Make sure the bot is in that server.", color=discord.Color.red())
            await ctx.send(embed=embed)
            return

        async with self.config.sync_groups() as groups:
            if group_name not in groups:
                embed = discord.Embed(title="Error", description=f"Group '{group_name}' not found.", color=discord.Color.red())
                await ctx.send(embed=embed)
                return
            if guild_id in groups[group_name]["slaves"]:
                 embed = discord.Embed(title="Error", description=f"Server {guild.name} (`{guild_id}`) is currently a slave in this group. Remove it as a slave first.", color=discord.Color.red())
                 await ctx.send(embed=embed)
                 return
            groups[group_name]["master"] = guild_id
        embed = discord.Embed(title="Master Server Set", description=f"Server **{guild.name}** (`{guild_id}`) set as master for sync group **'{group_name}'**.", color=discord.Color.green())
        await ctx.send(embed=embed)

    @rolesync.command(name="addslave")
    async def rolesync_addslave(self, ctx: commands.Context, group_name: str, guild_id: int):
        """Adds a slave server (by ID) to a sync group."""
        guild = self.bot.get_guild(guild_id)
        if not guild:
            embed = discord.Embed(title="Error", description=f"Could not find a server with ID `{guild_id}`. Make sure the bot is in that server.", color=discord.Color.red())
            await ctx.send(embed=embed)
            return

        async with self.config.sync_groups() as groups:
            if group_name not in groups:
                embed = discord.Embed(title="Error", description=f"Group '{group_name}' not found.", color=discord.Color.red())
                await ctx.send(embed=embed)
                return
            if groups[group_name]["master"] is None:
                embed = discord.Embed(title="Error", description=f"Group '{group_name}' does not have a master server set yet.", color=discord.Color.red())
                await ctx.send(embed=embed)
                return
            if guild_id == groups[group_name]["master"]:
                 embed = discord.Embed(title="Error", description=f"Server **{guild.name}** (`{guild_id}`) is the master for this group and cannot be a slave.", color=discord.Color.red())
                 await ctx.send(embed=embed)
                 return
            if guild_id in groups[group_name]["slaves"]:
                embed = discord.Embed(title="Info", description=f"Server **{guild.name}** (`{guild_id}`) is already a slave in group **'{group_name}'**.", color=discord.Color.blue())
                await ctx.send(embed=embed)
                return
            groups[group_name]["slaves"].append(guild_id)
        embed = discord.Embed(title="Slave Server Added", description=f"Server **{guild.name}** (`{guild_id}`) added as a slave to sync group **'{group_name}'**.", color=discord.Color.green())
        await ctx.send(embed=embed)

    @rolesync.command(name="removeslave")
    async def rolesync_removeslave(self, ctx: commands.Context, group_name: str, guild_id: int):
        """Removes a slave server (by ID) from a sync group."""
        guild_name_or_id = str(guild_id)
        guild = self.bot.get_guild(guild_id)
        if guild:
            guild_name_or_id = f"{guild.name} ({guild_id})"

        async with self.config.sync_groups() as groups:
            if group_name not in groups:
                embed = discord.Embed(title="Error", description=f"Group '{group_name}' not found.", color=discord.Color.red())
                await ctx.send(embed=embed)
                return
            if guild_id not in groups[group_name]["slaves"]:
                embed = discord.Embed(title="Error", description=f"Server {guild_name_or_id} is not a slave in group **'{group_name}'**.", color=discord.Color.red())
                await ctx.send(embed=embed)
                return
            groups[group_name]["slaves"].remove(guild_id)
        embed = discord.Embed(title="Slave Server Removed", description=f"Server **{guild_name_or_id}** removed as a slave from sync group **'{group_name}'**.", color=discord.Color.green())
        await ctx.send(embed=embed)


    @rolesync.command(name="addrole")
    async def rolesync_addrole(self, ctx: commands.Context, group_name: str, *, role_name: str):
        """Adds a role name to be synced for a specific group. Case-sensitive."""
        async with self.config.sync_groups() as groups:
            if group_name not in groups:
                embed = discord.Embed(title="Error", description=f"Group '{group_name}' not found.", color=discord.Color.red())
                await ctx.send(embed=embed)
                return
            if "roles" not in groups[group_name]:
                 groups[group_name]["roles"] = []
            if role_name in groups[group_name]["roles"]:
                embed = discord.Embed(title="Info", description=f"Role name `{role_name}` is already configured for sync in group **'{group_name}'**.", color=discord.Color.blue())
                await ctx.send(embed=embed)
                return
            groups[group_name]["roles"].append(role_name)
        embed = discord.Embed(title="Sync Role Added", description=f"Role name `{role_name}` will now be synced for group **'{group_name}'**.", color=discord.Color.green())
        await ctx.send(embed=embed)

    @rolesync.command(name="removerole")
    async def rolesync_removerole(self, ctx: commands.Context, group_name: str, *, role_name: str):
        """Removes a role name from being synced for a specific group."""
        async with self.config.sync_groups() as groups:
            if group_name not in groups:
                embed = discord.Embed(title="Error", description=f"Group '{group_name}' not found.", color=discord.Color.red())
                await ctx.send(embed=embed)
                return
            if role_name not in groups[group_name].get("roles", []):
                embed = discord.Embed(title="Error", description=f"Role name `{role_name}` is not configured for sync in group **'{group_name}'**.", color=discord.Color.red())
                await ctx.send(embed=embed)
                return
            groups[group_name]["roles"].remove(role_name)
        embed = discord.Embed(title="Sync Role Removed", description=f"Role name `{role_name}` will no longer be synced for group **'{group_name}'**.", color=discord.Color.green())
        await ctx.send(embed=embed)


    @rolesync.command(name="toggle")
    async def rolesync_toggle(self, ctx: commands.Context):
        """Toggles role synchronization globally."""
        current_status = await self.config.enabled()
        await self.config.enabled.set(not current_status)
        status = "enabled" if not current_status else "disabled"
        embed = discord.Embed(title="Role Synchronization Toggled", description=f"Role synchronization is now globally **{status}**.", color=discord.Color.green())
        await ctx.send(embed=embed)

    @rolesync.command(name="forcesync")
    async def rolesync_forcesync(self, ctx: commands.Context, group_name: Optional[str] = None):
        """Forces sync from master to slaves for configured roles.

        Optionally specify a group name to sync only that group.
        """
        if not await self.config.enabled():
            embed = discord.Embed(title="Error", description=f"Role synchronization is globally disabled. Enable it first with `{ctx.prefix}rolesync toggle`.", color=discord.Color.red())
            await ctx.send(embed=embed)
            return

        embed = discord.Embed(title="Force Sync Started", description="Starting role synchronization from master servers... This may take a while.", color=discord.Color.blue())
        initial_message = await ctx.send(embed=embed)
        all_groups: SyncGroups = await self.config.sync_groups()
        groups_to_sync = {}

        if group_name:
            if group_name in all_groups:
                groups_to_sync[group_name] = all_groups[group_name]
            else:
                embed = discord.Embed(title="Error", description=f"Group '{group_name}' not found.", color=discord.Color.red())
                await initial_message.edit(embed=embed)
                return
        else:
            groups_to_sync = all_groups

        if not groups_to_sync:
            embed = discord.Embed(title="Error", description="No sync groups configured or specified group not found.", color=discord.Color.red())
            await initial_message.edit(embed=embed)
            return

        processed_users = 0
        sync_actions = 0

        for g_name, group_data in groups_to_sync.items():
            master_id = group_data.get("master")
            slave_ids = group_data.get("slaves", [])
            allowed_roles = set(group_data.get("roles", []))

            if not master_id:
                log.info(f"Skipping group '{g_name}' in forcesync: No master server set.")
                continue
            if not slave_ids:
                log.info(f"Skipping group '{g_name}' in forcesync: No slave servers set.")
                continue
            if not allowed_roles:
                log.info(f"Skipping group '{g_name}' in forcesync: No roles configured.")
                continue

            master_guild = self.bot.get_guild(master_id)
            if not master_guild:
                log.warning(f"Skipping group '{g_name}' in forcesync: Master server {master_id} not found or bot not in it.")
                continue

            slave_guild_objects = [self.bot.get_guild(sid) for sid in slave_ids]
            slave_guild_objects = [g for g in slave_guild_objects if g]
            if not slave_guild_objects:
                 log.warning(f"Skipping group '{g_name}' in forcesync: No available slave servers found.")
                 continue

            log.info(f"Forcesync: Processing group '{g_name}' (Master: {master_guild.name})")

            for master_member in master_guild.members:
                if master_member.bot: continue

                master_roles_names = {r.name for r in master_member.roles}
                relevant_master_roles = master_roles_names.intersection(allowed_roles)

                for slave_guild in slave_guild_objects:
                    slave_member = slave_guild.get_member(master_member.id)
                    if slave_member:
                        processed_users += 1
                        slave_roles_names = {r.name for r in slave_member.roles}
                        relevant_slave_roles = slave_roles_names.intersection(allowed_roles)

                        to_add_slave = relevant_master_roles - relevant_slave_roles

                        to_remove_slave = relevant_slave_roles - relevant_master_roles

                        if to_add_slave or to_remove_slave:
                            log.debug(f"Forcesync {g_name}: Syncing {master_member} ({master_guild.name}) -> {slave_member} ({slave_guild.name}). Add: {to_add_slave}, Remove: {to_remove_slave}")
                            await self._sync_member_roles(master_member, master_guild, slave_member, slave_guild, to_add_slave, to_remove_slave, allowed_roles)
                            sync_actions += len(to_add_slave) + len(to_remove_slave)

        embed = discord.Embed(
            title="Force Sync Complete",
            description=f"Master->Slave role synchronization complete.\nProcessed **{processed_users}** user instances across master/slave pairs.\nPerformed **{sync_actions}** role adjustments based on configured roles.",
            color=discord.Color.green()
        )
        await initial_message.edit(embed=embed)

    @commands.Cog.listener()
    async def on_member_update(self, before: discord.Member, after: discord.Member):
        """Listens for role changes on a master server and syncs to slaves."""
        if not await self.config.enabled() or before.roles == after.roles or after.bot:
            return

        master_guild = after.guild
        master_member = after

        all_groups: SyncGroups = await self.config.sync_groups()
        group_data = None
        group_name = None

        for g_name, g_data in all_groups.items():
            if g_data.get("master") == master_guild.id:
                group_data = g_data
                group_name = g_name
                break

        if not group_data or not group_name:
            return

        slave_ids = group_data.get("slaves", [])
        allowed_roles = set(group_data.get("roles", []))

        if not slave_ids or not allowed_roles:
            return

        before_role_names = {r.name for r in before.roles}
        after_role_names = {r.name for r in after.roles}

        added_role_names = after_role_names - before_role_names
        removed_role_names = before_role_names - after_role_names

        relevant_added = added_role_names.intersection(allowed_roles)
        relevant_removed = removed_role_names.intersection(allowed_roles)

        if not relevant_added and not relevant_removed:
            return

        log.info(f"Detected relevant role change for {master_member} in master server {master_guild.name} (Group: {group_name}). Changes: Add {relevant_added}, Remove {relevant_removed}")

        for slave_id in slave_ids:
            slave_guild = self.bot.get_guild(slave_id)
            if not slave_guild:
                log.warning(f"Slave guild {slave_id} not found or bot not in it for group {group_name}.")
                continue

            slave_member = slave_guild.get_member(master_member.id)
            if not slave_member:
                continue

            log.debug(f"Syncing {master_member} ({master_guild.name}) -> {slave_member} ({slave_guild.name}). Add: {relevant_added}, Remove: {relevant_removed}")
            await self._sync_member_roles(master_member, master_guild, slave_member, slave_guild, relevant_added, relevant_removed, allowed_roles)

    async def _sync_member_roles(self, source_member: discord.Member, source_guild: discord.Guild,
                                 target_member: discord.Member, target_guild: discord.Guild,
                                 roles_to_add_names: Set[str], roles_to_remove_names: Set[str],
                                 allowed_roles: Set[str]):
        """Applies role changes TO the target_member based on changes from the source_member.

        Ensures only allowed roles are modified.
        """
        roles_to_add_target = []
        roles_to_remove_target = []

        final_roles_to_add = roles_to_add_names
        final_roles_to_remove = roles_to_remove_names

        for role_name in final_roles_to_add:
            role = discord.utils.get(target_guild.roles, name=role_name)
            if role and role not in target_member.roles:
                if target_guild.me.top_role > role:
                    roles_to_add_target.append(role)
                else:
                    log.warning(f"RoleSync: Cannot add role '{role.name}' to {target_member} in {target_guild.name} - Bot hierarchy too low.")
            elif not role:
                 log.warning(f"RoleSync: Role '{role_name}' to add not found in target guild {target_guild.name}.")


        for role_name in final_roles_to_remove:
            role = discord.utils.get(target_guild.roles, name=role_name)
            if role and role in target_member.roles:
                if target_guild.me.top_role > role:
                    roles_to_remove_target.append(role)
                else:
                    log.warning(f"RoleSync: Cannot remove role '{role.name}' from {target_member} in {target_guild.name} - Bot hierarchy too low.")

        try:
            if roles_to_add_target:
                await target_member.add_roles(*roles_to_add_target, reason=f"RoleSync from master {source_guild.name}")
                log.info(f"Added roles {[r.name for r in roles_to_add_target]} to {target_member} in slave {target_guild.name}")
            if roles_to_remove_target:
                await target_member.remove_roles(*roles_to_remove_target, reason=f"RoleSync from master {source_guild.name}")
                log.info(f"Removed roles {[r.name for r in roles_to_remove_target]} from {target_member} in slave {target_guild.name}")
        except discord.Forbidden:
            log.error(f"RoleSync: Missing permissions to modify roles for {target_member} in {target_guild.name}.")
        except discord.HTTPException as e:
            log.error(f"RoleSync: Failed to modify roles for {target_member} in {target_guild.name}: {e}")
