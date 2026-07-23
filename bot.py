#!/usr/bin/env python3
"""
VELTREX VPS Manager & Moderation Bot
Features: VPS Management (CodeSandbox Compatible), Moderation, Utilities, & Welcome System.
"""

import os
import sys
import json
import time
import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import discord
from discord import app_commands
from discord.ext import commands

import docker
from docker.errors import NotFound

# ==========================================
# LOGGING CONFIGURATION
# ==========================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler("bot.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("VELTREXBot")


# ==========================================
# CONFIGURATION MANAGER
# ==========================================
class ConfigManager:
    def __init__(self, filepath: str = "config.json"):
        self.filepath = filepath
        self.config: Dict[str, Any] = {}
        self.load_config()

    def load_config(self) -> None:
        if not os.path.exists(self.filepath):
            default_config = {
                "token": "YOUR_DISCORD_BOT_TOKEN_HERE",
                "guild_id": None,
                "admin_ids": [],
                "welcome_channel_id": None,
                "database": {"file": "database.json"}
            }
            try:
                with open(self.filepath, "w", encoding="utf-8") as f:
                    json.dump(default_config, f, indent=4)
                self.config = default_config
            except Exception as err:
                logger.error(f"Could not create default config: {err}")
                self.config = {}
            return

        try:
            with open(self.filepath, "r", encoding="utf-8") as f:
                self.config = json.load(f)
        except json.JSONDecodeError as e:
            logger.critical(f"Config corrupted: {e}")
            self.config = {}

    def set(self, key: str, value: Any) -> None:
        self.config[key] = value
        try:
            with open(self.filepath, "w", encoding="utf-8") as f:
                json.dump(self.config, f, indent=4)
        except Exception as e:
            logger.error(f"Failed to update config key '{key}': {e}")

    @property
    def token(self) -> str:
        return (os.getenv("DISCORD_TOKEN") or os.getenv("TOKEN") or self.config.get("token") or "").strip()

    @property
    def guild_id(self) -> Optional[int]:
        gid = self.config.get("guild_id")
        return int(gid) if gid else None

    @property
    def admin_ids(self) -> List[int]:
        raw_ids = self.config.get("admin_ids", [])
        if isinstance(raw_ids, int):
            return [raw_ids]
        return [int(uid) for uid in raw_ids if str(uid).isdigit()]

    @property
    def welcome_channel_id(self) -> Optional[int]:
        cid = self.config.get("welcome_channel_id")
        return int(cid) if cid else None

    @property
    def db_file(self) -> str:
        return self.config.get("database", {}).get("file", "database.json")


# ==========================================
# DATABASE MANAGER
# ==========================================
class DatabaseManager:
    def __init__(self, filepath: str):
        self.filepath = filepath
        self.lock = asyncio.Lock()
        self._init_db()

    def _init_db(self) -> None:
        if not os.path.exists(self.filepath):
            try:
                with open(self.filepath, "w", encoding="utf-8") as f:
                    json.dump({"vps_records": {}}, f, indent=4)
            except Exception as e:
                logger.error(f"Failed to create DB: {e}")

    async def _read(self) -> Dict[str, Any]:
        async with self.lock:
            if not os.path.exists(self.filepath):
                return {"vps_records": {}}
            try:
                with open(self.filepath, "r", encoding="utf-8") as f:
                    return json.load(f)
            except json.JSONDecodeError:
                return {"vps_records": {}}

    async def _write(self, data: Dict[str, Any]) -> None:
        async with self.lock:
            try:
                with open(self.filepath, "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=4)
            except Exception as e:
                logger.error(f"Failed to write DB: {e}")

    async def get_all_vps(self) -> Dict[str, Any]:
        data = await self._read()
        return data.get("vps_records", {})

    async def get_vps(self, name: str) -> Optional[Dict[str, Any]]:
        records = await self.get_all_vps()
        return records.get(name.lower())

    async def save_vps(self, name: str, vps_data: Dict[str, Any]) -> None:
        data = await self._read()
        if "vps_records" not in data:
            data["vps_records"] = {}
        data["vps_records"][name.lower()] = vps_data
        await self._write(data)

    async def delete_vps(self, name: str) -> None:
        data = await self._read()
        if "vps_records" in data and name.lower() in data["vps_records"]:
            del data["vps_records"][name.lower()]
            await self._write(data)


# ==========================================
# DOCKER & TMATE MANAGER
# ==========================================
class DockerManager:
    def __init__(self, config_mgr: ConfigManager):
        self.config = config_mgr
        self.client: Optional[docker.DockerClient] = None
        self._connect()

    def _connect(self) -> None:
        try:
            self.client = docker.from_env()
            self.client.ping()
            logger.info("Docker Daemon connected successfully.")
        except Exception as e:
            logger.warning(f"Docker connection failed: {e}")
            self.client = None

    async def create_tmate_container(self, name: str, ram: str, cpu: float) -> tuple[str, str]:
        def _sync_create() -> tuple[str, str]:
            if not self.client:
                raise RuntimeError("Docker client is unavailable.")

            image_name = "ubuntu:22.04"
            try:
                self.client.images.get(image_name)
            except NotFound:
                logger.info(f"Pulling base image '{image_name}'...")
                self.client.images.pull(image_name)

            nano_cpus = int(cpu * 1_000_000_000)

            # CodeSandbox-safe execution
            startup_cmd = (
                "bash -c '"
                "export DEBIAN_FRONTEND=noninteractive && "
                "apt-get update -qq && apt-get install -y -qq tmate openssh-client curl > /dev/null 2>&1 && "
                "ssh-keygen -q -t rsa -N \"\" -f ~/.ssh/id_rsa && "
                "tmate -S /tmp/tmate.sock new-session -d && "
                "tmate -S /tmp/tmate.sock wait tmate-ready && "
                "tmate -S /tmp/tmate.sock display -p \"#{tmate_ssh}\" > /tmp/tmate_ssh.txt && "
                "tail -f /dev/null"
                "'"
            )

            container = self.client.containers.run(
                image=image_name,
                command=startup_cmd,
                name=f"vps_{name}",
                detach=True,
                mem_limit=ram,
                nano_cpus=nano_cpus,
                restart_policy={"Name": "unless-stopped"}
            )

            tmate_ssh = ""
            for _ in range(60):
                time.sleep(2)
                try:
                    exec_res = container.exec_run("cat /tmp/tmate_ssh.txt")
                    if exec_res.exit_code == 0:
                        output = exec_res.output.decode().strip()
                        if "ssh" in output or "tmate.io" in output:
                            tmate_ssh = output
                            break
                except Exception:
                    pass

            if not tmate_ssh:
                logs = container.logs().decode('utf-8', errors='ignore')
                container.remove(force=True)
                raise RuntimeError(f"Connection timed out. Logs:\n```{logs[-300:]}```")

            return str(container.id), tmate_ssh

        return await asyncio.to_thread(_sync_create)

    async def remove_container(self, container_id: str) -> None:
        def _sync_remove() -> None:
            if not self.client:
                return
            try:
                container = self.client.containers.get(container_id)
                container.remove(force=True)
            except NotFound:
                pass

        await asyncio.to_thread(_sync_remove)


# ==========================================
# DISCORD BOT CLIENT
# ==========================================
class VPSBot(commands.Bot):
    def __init__(self, cfg: ConfigManager, db: DatabaseManager, dckr: DockerManager):
        intents = discord.Intents.default()
        intents.members = True
        intents.message_content = True
        super().__init__(command_prefix="!", intents=intents)
        self.cfg = cfg
        self.db = db
        self.docker = dckr

    async def setup_hook(self) -> None:
        if self.cfg.guild_id:
            guild = discord.Object(id=self.cfg.guild_id)
            self.tree.copy_global_to(guild=guild)
            await self.tree.sync(guild=guild)
        else:
            await self.tree.sync()

    async def on_ready(self) -> None:
        logger.info(f"Bot Active as: {self.user}")

    async def on_member_join(self, member: discord.Member) -> None:
        channel_id = self.cfg.welcome_channel_id
        if not channel_id:
            return

        channel = member.guild.get_channel(channel_id)
        if isinstance(channel, discord.TextChannel):
            welcome_embed = discord.Embed(
                title=f"Welcome to {member.guild.name}!",
                description=f"Hey {member.mention}, welcome to the server! We are glad to have you here.",
                color=discord.Color.blue()
            )
            welcome_embed.set_thumbnail(url=member.display_avatar.url)
            welcome_embed.set_footer(text=f"User #{member.guild.member_count}")
            await channel.send(embed=welcome_embed)


# Initialize global managers
config_manager = ConfigManager()
db_manager = DatabaseManager(config_manager.db_file)
docker_manager = DockerManager(config_manager)
bot = VPSBot(config_manager, db_manager, docker_manager)


def is_admin(interaction: discord.Interaction) -> bool:
    return interaction.user.id in config_manager.admin_ids or interaction.user.guild_permissions.administrator


# ==========================================
# VPS COMMANDS
# ==========================================
@bot.tree.command(name="createvps", description="Provision a VPS instance with tmate SSH.")
async def createvps(
    interaction: discord.Interaction,
    user: discord.User,
    name: str,
    ram: str = "3g",
    cpu: float = 1.0,
    disk: str = "10G"
) -> None:
    if not is_admin(interaction):
        await interaction.response.send_message("🚫 Access Denied", ephemeral=True)
        return

    await interaction.response.defer()
    clean_name = "".join(c for c in name if c.isalnum() or c in ('-', '_')).lower()

    try:
        container_id, tmate_ssh = await docker_manager.create_tmate_container(
            name=clean_name,
            ram=ram,
            cpu=cpu
        )

        vps_record = {
            "owner_id": user.id,
            "vps_name": clean_name,
            "container_id": container_id,
            "creation_time": datetime.now(timezone.utc).isoformat(),
            "ram_limit": ram,
            "cpu_limit": cpu,
            "disk": disk,
            "tmate_ssh": tmate_ssh
        }
        await db_manager.save_vps(clean_name, vps_record)

        vps_embed = discord.Embed(
            title="VPS Instance Created",
            color=discord.Color.from_rgb(46, 204, 113)
        )
        cpu_display = int(cpu) if cpu.is_integer() else cpu
        vps_embed.description = (
            f"OS: Ubuntu 22.04\n"
            f"RAM: {ram} | CPU: {cpu_display} | Disk: {disk}\n"
            f"```\n{tmate_ssh}\n```"
        )
        vps_embed.set_footer(text=f"Powered by VELTREX VPS Bot | {datetime.now().strftime('%m/%d/%Y %I:%M %p')}")

        try:
            await user.send(embed=vps_embed)
            await interaction.followup.send(content=f"✅ VPS `{clean_name}` created and sent to <@{user.id}>!")
        except discord.Forbidden:
            await interaction.followup.send(content=f"⚠️ VPS created, but target user DMs were closed:", embed=vps_embed)

    except Exception as e:
        err_embed = discord.Embed(title="❌ Deployment Error", description=str(e), color=discord.Color.red())
        await interaction.followup.send(embed=err_embed)


@bot.tree.command(name="info", description="View connection info for a VPS instance.")
async def info(interaction: discord.Interaction, name: str) -> None:
    clean_name = name.lower()
    vps_data = await db_manager.get_vps(clean_name)

    if not vps_data:
        await interaction.response.send_message("❌ VPS instance not found.", ephemeral=True)
        return

    vps_embed = discord.Embed(
        title="VPS Instance Details",
        color=discord.Color.from_rgb(46, 204, 113)
    )
    vps_embed.description = (
        f"OS: Ubuntu 22.04\n"
        f"RAM: {vps_data.get('ram_limit')} | CPU: {vps_data.get('cpu_limit')} | Disk: {vps_data.get('disk')}\n"
        f"```\n{vps_data.get('tmate_ssh')}\n```"
    )
    await interaction.response.send_message(embed=vps_embed)


@bot.tree.command(name="deletevps", description="Delete an active VPS instance.")
async def deletevps(interaction: discord.Interaction, name: str) -> None:
    if not is_admin(interaction):
        await interaction.response.send_message("🚫 Access Denied", ephemeral=True)
        return

    clean_name = name.lower()
    vps_data = await db_manager.get_vps(clean_name)

    if not vps_data:
        await interaction.response.send_message("❌ VPS not found.", ephemeral=True)
        return

    await interaction.response.defer()
    await docker_manager.remove_container(vps_data.get("container_id", ""))
    await db_manager.delete_vps(clean_name)
    await interaction.followup.send(content=f"🗑️ VPS `{clean_name}` has been successfully removed.")


# ==========================================
# UTILITY COMMANDS
# ==========================================
@bot.tree.command(name="ping", description="Check bot status and latency.")
async def ping(interaction: discord.Interaction) -> None:
    latency = round(bot.latency * 1000)
    await interaction.response.send_message(f"🏓 Pong! Latency: `{latency}ms`")


@bot.tree.command(name="userinfo", description="Display details about a user.")
async def userinfo(interaction: discord.Interaction, member: Optional[discord.Member] = None) -> None:
    target = member or interaction.user
    embed = discord.Embed(title=f"User Info - {target.name}", color=discord.Color.blue())
    embed.set_thumbnail(url=target.display_avatar.url)
    embed.add_field(name="User ID", value=str(target.id), inline=True)
    embed.add_field(name="Joined Server", value=target.joined_at.strftime("%Y-%m-%d") if target.joined_at else "N/A", inline=True)
    embed.add_field(name="Account Created", value=target.created_at.strftime("%Y-%m-%d"), inline=True)
    await interaction.response.send_message(embed=embed)


@bot.tree.command(name="serverinfo", description="Display information about this server.")
async def serverinfo(interaction: discord.Interaction) -> None:
    guild = interaction.guild
    if not guild:
        await interaction.response.send_message("❌ Cannot execute outside of a server.", ephemeral=True)
        return

    embed = discord.Embed(title=f"Server Info - {guild.name}", color=discord.Color.green())
    if guild.icon:
        embed.set_thumbnail(url=guild.icon.url)
    embed.add_field(name="Members", value=str(guild.member_count), inline=True)
    embed.add_field(name="Channels", value=str(len(guild.channels)), inline=True)
    embed.add_field(name="Roles", value=str(len(guild.roles)), inline=True)
    embed.add_field(name="Created On", value=guild.created_at.strftime("%Y-%m-%d"), inline=True)
    await interaction.response.send_message(embed=embed)


# ==========================================
# MODERATION & WELCOME COMMANDS
# ==========================================
@bot.tree.command(name="kick", description="Kick a member from the server.")
@app_commands.checks.has_permissions(kick_members=True)
async def kick(interaction: discord.Interaction, member: discord.Member, reason: Optional[str] = "No reason provided.") -> None:
    await member.kick(reason=reason)
    await interaction.response.send_message(f"👞 Kicked {member.mention}. Reason: {reason}")


@bot.tree.command(name="ban", description="Ban a member from the server.")
@app_commands.checks.has_permissions(ban_members=True)
async def ban(interaction: discord.Interaction, member: discord.Member, reason: Optional[str] = "No reason provided.") -> None:
    await member.ban(reason=reason)
    await interaction.response.send_message(f"🔨 Banned {member.mention}. Reason: {reason}")


@bot.tree.command(name="purge", description="Clear a specified number of messages in the channel.")
@app_commands.checks.has_permissions(manage_messages=True)
async def purge(interaction: discord.Interaction, amount: int) -> None:
    if amount < 1:
        await interaction.response.send_message("Amount must be at least 1.", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)
    deleted = await interaction.channel.purge(limit=amount)
    await interaction.followup.send(content=f"🧹 Cleared {len(deleted)} messages.")


@bot.tree.command(name="setwelcome", description="Set the welcome channel for new members.")
@app_commands.checks.has_permissions(administrator=True)
async def setwelcome(interaction: discord.Interaction, channel: discord.TextChannel) -> None:
    config_manager.set("welcome_channel_id", channel.id)
    await interaction.response.send_message(f"✅ Welcome channel has been set to {channel.mention}.")


# ==========================================
# MAIN ENTRY
# ==========================================
if __name__ == "__main__":
    token = config_manager.token
    if token:
        bot.run(token)
    else:
        logger.critical("No Discord bot token found! Please set DISCORD_TOKEN in your environment or config.json.")
            
