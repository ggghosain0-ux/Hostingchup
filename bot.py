#!/usr/bin/env python3
"""
Discord VPS Manager Bot (tmate.io Integration + Moderation & Utilities)
Uses local Docker containers as real VPS instances exposed via tmate.io sessions.
"""

import os
import sys
import json
import time
import socket
import string
import secrets
import logging
import traceback
import asyncio
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import discord
from discord import app_commands
from discord.ext import commands

import docker
from docker.errors import DockerException, APIError, NotFound

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
logger = logging.getLogger("VPSManagerBot")


# ==========================================
# CONFIGURATION MANAGER
# ==========================================
class ConfigManager:
    """Loads settings from config.json or environment variables."""
    def __init__(self, filepath: str = "config.json"):
        self.filepath = filepath
        self.config: Dict[str, Any] = {}
        self.load_config()

    def load_config(self) -> None:
        if not os.path.exists(self.filepath):
            logger.warning(f"Configuration file '{self.filepath}' missing. Creating default.")
            default_config = {
                "token": "YOUR_DISCORD_BOT_TOKEN_HERE",
                "guild_id": None,
                "admin_ids": [],
                "database": {"file": "database.json"},
                "limits": {
                    "default_ram": "512m",
                    "default_cpu": 0.5
                }
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
            logger.info("Configuration loaded.")
        except json.JSONDecodeError as e:
            logger.critical(f"Config JSON corrupted: {e}")
            self.config = {}

    @property
    def token(self) -> str:
        env_token = os.getenv("DISCORD_TOKEN") or os.getenv("TOKEN")
        if env_token and env_token.strip():
            return env_token.strip()
        raw_token = self.config.get("token") or self.config.get("bot_token")
        if isinstance(raw_token, str):
            return raw_token.strip()
        return ""

    @property
    def guild_id(self) -> Optional[int]:
        gid = self.config.get("guild_id")
        try:
            return int(gid) if gid else None
        except (ValueError, TypeError):
            return None

    @property
    def admin_ids(self) -> List[int]:
        raw_ids = self.config.get("admin_ids", [])
        if isinstance(raw_ids, int):
            return [raw_ids]
        result = []
        if isinstance(raw_ids, list):
            for uid in raw_ids:
                try:
                    result.append(int(uid))
                except (ValueError, TypeError):
                    pass
        return result

    @property
    def db_file(self) -> str:
        return self.config.get("database", {}).get("file", "database.json")

    @property
    def default_ram(self) -> str:
        return self.config.get("limits", {}).get("default_ram", "512m")

    @property
    def default_cpu(self) -> float:
        try:
            return float(self.config.get("limits", {}).get("default_cpu", 0.5))
        except ValueError:
            return 0.5


# ==========================================
# DATABASE MANAGER
# ==========================================
class DatabaseManager:
    """JSON persistent storage manager."""
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
                    data = json.load(f)
                    return data if isinstance(data, dict) else {"vps_records": {}}
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
        key = name.lower()
        if "vps_records" in data and key in data["vps_records"]:
            del data["vps_records"][key]
            await self._write(data)


# ==========================================
# DOCKER & TMATE MANAGER
# ==========================================
class DockerManager:
    """Handles container creation and tmate session extraction."""
    def __init__(self, config_mgr: ConfigManager):
        self.config = config_mgr
        self.client: Optional[docker.DockerClient] = None
        self._connect()

    def _connect(self) -> None:
        try:
            self.client = docker.from_env()
            self.client.ping()
            logger.info("Docker Daemon connected.")
        except Exception as e:
            logger.warning(f"Docker connection failed: {e}")
            self.client = None

    def is_available(self) -> bool:
        if self.client is None:
            self._connect()
        if self.client is None:
            return False
        try:
            return bool(self.client.ping())
        except DockerException:
            return False

    async def create_tmate_container(self, name: str, ram: str, cpu: float) -> tuple[str, str]:
        """Creates container, starts tmate, and returns (container_id, tmate_ssh_string)."""
        def _sync_create() -> tuple[str, str]:
            if not self.client:
                raise RuntimeError("Docker client unavailable.")

            image_name = "ubuntu:24.04"
            try:
                self.client.images.get(image_name)
            except NotFound:
                logger.info(f"Pulling image '{image_name}'...")
                self.client.images.pull(image_name)

            nano_cpus = int(cpu * 1_000_000_000)

            # Boot command: Install tmate, start session, and sleep infinity
            startup_script = (
                "apt-get update && apt-get install -y tmate curl openssh-client && "
                "tmate -F -s /tmp/tmate.sock new-session -d && "
                "tmate -s /tmp/tmate.sock wait tmate-ready && "
                "tmate -s /tmp/tmate.sock display -p '#{tmate_ssh}' > /tmp/tmate_ssh.txt && "
                "sleep infinity"
            )

            container = self.client.containers.run(
                image=image_name,
                command=f"/bin/bash -c \"{startup_script}\"",
                name=f"vps_{name}",
                detach=True,
                mem_limit=ram,
                nano_cpus=nano_cpus,
                restart_policy={"Name": "unless-stopped"}
            )

            # Wait for tmate session to register and print SSH key string
            tmate_ssh = ""
            for _ in range(30):
                time.sleep(1)
                try:
                    exec_res = container.exec_run("cat /tmp/tmate_ssh.txt")
                    if exec_res.exit_code == 0:
                        output = exec_res.output.decode().strip()
                        if "tmate.io" in output:
                            tmate_ssh = output
                            break
                except Exception:
                    pass

            if not tmate_ssh:
                tmate_ssh = "tmate session startup timed out. Run 'tmate -s /tmp/tmate.sock display -p \"#{tmate_ssh}\"' inside container."

            return str(container.id), tmate_ssh

        return await asyncio.to_thread(_sync_create)

    async def manage_container(self, container_id: str, action: str) -> None:
        def _sync_manage() -> None:
            if not self.client:
                raise RuntimeError("Docker client unavailable.")
            try:
                container = self.client.containers.get(container_id)
                if action == "start":
                    container.start()
                elif action == "stop":
                    container.stop(timeout=5)
                elif action == "restart":
                    container.restart(timeout=5)
                elif action == "delete":
                    container.remove(force=True)
            except NotFound:
                raise ValueError("Container not found.")
            except APIError as e:
                raise RuntimeError(f"Docker API Error: {e}")

        await asyncio.to_thread(_sync_manage)

    async def get_container_status(self, container_id: str) -> str:
        def _sync_status() -> str:
            if not self.client:
                return "offline"
            try:
                c = self.client.containers.get(container_id)
                return str(c.status)
            except NotFound:
                return "not found"
            except Exception:
                return "unknown"

        return await asyncio.to_thread(_sync_status)


# ==========================================
# DISCORD BOT INITIALIZATION
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
        logger.info("Synchronizing commands...")
        if self.cfg.guild_id:
            guild = discord.Object(id=self.cfg.guild_id)
            self.tree.copy_global_to(guild=guild)
            await self.tree.sync(guild=guild)
            logger.info(f"Commands synced to guild: {guild.id}")
        else:
            await self.tree.sync()
            logger.info("Global commands synced.")

    async def on_ready(self) -> None:
        logger.info("=" * 50)
        logger.info(f"Bot Active: {self.user} (ID: {self.user.id if self.user else 'Unknown'})")
        logger.info("=" * 50)

        await self.change_presence(
            activity=discord.Activity(
                type=discord.ActivityType.watching,
                name="VPS Instances & tmate.io"
            )
        )

    async def on_member_join(self, member: discord.Member) -> None:
        """Sends welcome message when a new user joins."""
        channel = member.guild.system_channel
        if not channel:
            channel = next((ch for ch in member.guild.text_channels if ch.permissions_for(member.guild.me).send_messages), None)

        if channel:
            embed = discord.Embed(
                title=f"👋 Welcome to {member.guild.name}!",
                description=f"Welcome {member.mention}! Enjoy your stay and type `/help` to see available commands.",
                color=discord.Color.blue()
            )
            embed.set_thumbnail(url=member.display_avatar.url)
            await channel.send(embed=embed)


config_manager = ConfigManager()
db_manager = DatabaseManager(config_manager.db_file)
docker_manager = DockerManager(config_manager)
bot = VPSBot(config_manager, db_manager, docker_manager)


# ==========================================
# HELPERS
# ==========================================
def is_admin(interaction: discord.Interaction) -> bool:
    return interaction.user.id in config_manager.admin_ids or interaction.user.guild_permissions.administrator


async def send_access_denied(interaction: discord.Interaction) -> None:
    embed = discord.Embed(
        title="🚫 Access Denied",
        description="You do not have permission to execute this command.",
        color=discord.Color.red()
    )
    if interaction.response.is_done():
        await interaction.followup.send(embed=embed, ephemeral=True)
    else:
        await interaction.response.send_message(embed=embed, ephemeral=True)


# ==========================================
# VPS MANAGEMENT COMMANDS (TMATE ONLY)
# ==========================================

@bot.tree.command(name="createvps", description="Provision a VPS instance with a tmate SSH key for a target user.")
@app_commands.describe(
    user="The user who will own this VPS",
    name="Name of the VPS instance",
    ram="RAM allocation (e.g., 512m, 1g)",
    cpu="CPU allocation (e.g., 0.5, 1.0)"
)
async def createvps(
    interaction: discord.Interaction,
    user: discord.User,
    name: str,
    ram: str | None = None,
    cpu: float | None = None
) -> None:
    if not is_admin(interaction):
        await send_access_denied(interaction)
        return

    await interaction.response.defer()

    if not docker_manager.is_available():
        embed = discord.Embed(
            title="❌ Docker Daemon Offline",
            description="Docker is not running on the host system.",
            color=discord.Color.red()
        )
        await interaction.followup.send(embed=embed)
        return

    clean_name = "".join(c for c in name if c.isalnum() or c in ('-', '_')).lower()
    if not clean_name:
        embed = discord.Embed(title="❌ Invalid Name", description="Enter an alphanumeric name.", color=discord.Color.red())
        await interaction.followup.send(embed=embed)
        return

    existing = await db_manager.get_vps(clean_name)
    if existing:
        embed = discord.Embed(title="❌ Name Exists", description=f"`{clean_name}` already exists.", color=discord.Color.red())
        await interaction.followup.send(embed=embed)
        return

    allocated_ram = ram if ram else config_manager.default_ram
    allocated_cpu = cpu if cpu is not None else config_manager.default_cpu

    try:
        container_id, tmate_ssh = await docker_manager.create_tmate_container(
            name=clean_name,
            ram=allocated_ram,
            cpu=allocated_cpu
        )

        vps_record = {
            "owner_id": user.id,
            "vps_name": clean_name,
            "container_id": container_id,
            "creation_time": datetime.now(timezone.utc).isoformat(),
            "ram_limit": allocated_ram,
            "cpu_limit": allocated_cpu,
            "tmate_ssh": tmate_ssh,
            "status": "running"
        }

        await db_manager.save_vps(clean_name, vps_record)

        # User DM Embed
        dm_embed = discord.Embed(
            title="🚀 Your VPS Session is Ready!",
            description="Your instance was provisioned with a **tmate.io** SSH key session.",
            color=discord.Color.green()
        )
        dm_embed.add_field(name="Instance Name", value=f"`{clean_name}`", inline=True)
        dm_embed.add_field(name="RAM Limit", value=f"`{allocated_ram}`", inline=True)
        dm_embed.add_field(name="CPU Cores", value=f"`{allocated_cpu}`", inline=True)
        dm_embed.add_field(
            name="🔑 tmate SSH Connection Access Key",
            value=f"```bash\n{tmate_ssh}\n```",
            inline=False
        )
        dm_embed.set_footer(text="Connect from any terminal worldwide without port forwarding.")

        dm_sent = False
        try:
            await user.send(embed=dm_embed)
            dm_sent = True
        except discord.Forbidden:
            logger.warning(f"Failed to DM user {user.id}")

        confirm_embed = discord.Embed(
            title="🚀 VPS Provisioned",
            description=f"Created instance `{clean_name}` for <@{user.id}>.",
            color=discord.Color.green()
        )
        confirm_embed.add_field(name="Owner", value=f"<@{user.id}>", inline=True)
        confirm_embed.add_field(name="Container ID", value=f"`{container_id[:12]}`", inline=True)

        if dm_sent:
            confirm_embed.add_field(
                name="📩 DM Status",
                value=f"✅ Access key sent directly to <@{user.id}> via Direct Message.",
                inline=False
            )
        else:
            confirm_embed.add_field(
                name="⚠️ DM Failed (DMs Closed)",
                value=f"Could not DM <@{user.id}>.\n**tmate Access Key:**\n```bash\n{tmate_ssh}\n```",
                inline=False
            )

        await interaction.followup.send(embed=confirm_embed)

    except Exception as e:
        logger.exception("Deployment failed")
        embed = discord.Embed(title="❌ Deployment Error", description=str(e), color=discord.Color.red())
        await interaction.followup.send(embed=embed)


@bot.tree.command(name="deletevps", description="Delete an existing VPS instance.")
@app_commands.describe(name="Name of VPS to remove")
async def deletevps(interaction: discord.Interaction, name: str) -> None:
    if not is_admin(interaction):
        await send_access_denied(interaction)
        return

    await interaction.response.defer()
    clean_name = name.lower()
    vps_data = await db_manager.get_vps(clean_name)

    if not vps_data:
        embed = discord.Embed(title="❌ Not Found", description=f"No VPS named `{clean_name}`.", color=discord.Color.red())
        await interaction.followup.send(embed=embed)
        return

    try:
        if docker_manager.is_available():
            await docker_manager.manage_container(vps_data["container_id"], "delete")
        await db_manager.delete_vps(clean_name)

        embed = discord.Embed(title="🗑️ VPS Deleted", description=f"Removed `{clean_name}`.", color=discord.Color.green())
        await interaction.followup.send(embed=embed)
    except Exception as e:
        await db_manager.delete_vps(clean_name)
        embed = discord.Embed(title="⚠️ Warning", description=f"Removed record, but Docker error: `{e}`", color=discord.Color.orange())
        await interaction.followup.send(embed=embed)


@bot.tree.command(name="listvps", description="List all active VPS instances.")
async def listvps(interaction: discord.Interaction) -> None:
    await interaction.response.defer()
    records = await db_manager.get_all_vps()

    if not records:
        embed = discord.Embed(title="📋 VPS List", description="No instances found.", color=discord.Color.blue())
        await interaction.followup.send(embed=embed)
        return

    embed = discord.Embed(title="📋 Active VPS Instances", color=discord.Color.blue())
    for name, data in records.items():
        if not isinstance(data, dict):
            continue
        status = await docker_manager.get_container_status(data.get("container_id", ""))
        desc = (
            f"**Owner:** <@{data.get('owner_id')}>\n"
            f"**Container ID:** `{data.get('container_id', '')[:12]}`\n"
            f"**Status:** `{status.upper()}`\n"
            f"**RAM:** `{data.get('ram_limit')}` | **CPU:** `{data.get('cpu_limit')}`"
        )
        embed.add_field(name=f"🖥️ {name}", value=desc, inline=False)

    await interaction.followup.send(embed=embed)


@bot.tree.command(name="info", description="View info and tmate connection string for a VPS.")
@app_commands.describe(name="VPS name")
async def info(interaction: discord.Interaction, name: str) -> None:
    clean_name = name.lower()
    vps_data = await db_manager.get_vps(clean_name)

    if not vps_data:
        embed = discord.Embed(title="❌ Not Found", description=f"No VPS named `{clean_name}`.", color=discord.Color.red())
        await interaction.response.send_message(embed=embed)
        return

    if not is_admin(interaction) and interaction.user.id != vps_data.get("owner_id"):
        await send_access_denied(interaction)
        return

    status = await docker_manager.get_container_status(vps_data.get("container_id", ""))

    embed = discord.Embed(title=f"🛠️ Details: {clean_name}", color=discord.Color.purple())
    embed.add_field(name="Owner", value=f"<@{vps_data.get('owner_id')}>", inline=True)
    embed.add_field(name="Status", value=status.upper(), inline=True)
    embed.add_field(name="CPU / RAM", value=f"{vps_data.get('cpu_limit')} cores / {vps_data.get('ram_limit')}", inline=True)
    embed.add_field(
        name="🔑 tmate SSH Access Command",
        value=f"```bash\n{vps_data.get('tmate_ssh')}\n```",
        inline=False
    )
    await interaction.response.send_message(embed=embed)


# ==========================================
# MODERATION COMMANDS
# ==========================================

@bot.tree.command(name="kick", description="Kick a member from the server.")
@app_commands.describe(user="User to kick", reason="Reason for kicking")
async def kick(interaction: discord.Interaction, user: discord.Member, reason: Optional[str] = None) -> None:
    if not interaction.user.guild_permissions.kick_members:
        await send_access_denied(interaction)
        return

    try:
        await user.kick(reason=reason)
        embed = discord.Embed(
            title="👢 Member Kicked",
            description=f"Kicked {user.mention}. Reason: {reason or 'No reason provided.'}",
            color=discord.Color.orange()
        )
        await interaction.response.send_message(embed=embed)
    except Exception as e:
        await interaction.response.send_message(content=f"Failed to kick user: {e}", ephemeral=True)


@bot.tree.command(name="ban", description="Ban a member from the server.")
@app_commands.describe(user="User to ban", reason="Reason for banning")
async def ban(interaction: discord.Interaction, user: discord.Member, reason: Optional[str] = None) -> None:
    if not interaction.user.guild_permissions.ban_members:
        await send_access_denied(interaction)
        return

    try:
        await user.ban(reason=reason)
        embed = discord.Embed(
            title="🔨 Member Banned",
            description=f"Banned {user.mention}. Reason: {reason or 'No reason provided.'}",
            color=discord.Color.red()
        )
        await interaction.response.send_message(embed=embed)
    except Exception as e:
        await interaction.response.send_message(content=f"Failed to ban user: {e}", ephemeral=True)


@bot.tree.command(name="purge", description="Bulk delete messages in channel.")
@app_commands.describe(amount="Number of messages to clear (1-100)")
async def purge(interaction: discord.Interaction, amount: int) -> None:
    if not interaction.user.guild_permissions.manage_messages:
        await send_access_denied(interaction)
        return

    if amount < 1 or amount > 100:
        await interaction.response.send_message("Please specify an amount between 1 and 100.", ephemeral=True)
        return

    await interaction.response.defer(ephemeral=True)
    deleted = await interaction.channel.purge(limit=amount)
    await interaction.followup.send(f"🧹 Successfully cleared {len(deleted)} messages.", ephemeral=True)


# ==========================================
# UTILITY COMMANDS
# ==========================================

@bot.tree.command(name="ping", description="Check bot latency.")
async def ping(interaction: discord.Interaction) -> None:
    latency = round(bot.latency * 1000)
    embed = discord.Embed(title="🏓 Pong!", description=f"Latency: `{latency}ms`", color=discord.Color.green())
    await interaction.response.send_message(embed=embed)


@bot.tree.command(name="userinfo", description="Display user information.")
@app_commands.describe(user="Target user")
async def userinfo(interaction: discord.Interaction, user: Optional[discord.Member] = None) -> None:
    target = user or interaction.user
    embed = discord.Embed(title=f"👤 User Info: {target.name}", color=discord.Color.blue())
    embed.set_thumbnail(url=target.display_avatar.url)
    embed.add_field(name="ID", value=str(target.id), inline=True)
    embed.add_field(name="Joined Server", value=target.joined_at.strftime("%Y-%m-%d") if target.joined_at else "N/A", inline=True)
    embed.add_field(name="Account Created", value=target.created_at.strftime("%Y-%m-%d"), inline=True)
    await interaction.response.send_message(embed=embed)


@bot.tree.command(name="serverinfo", description="Display server statistics.")
async def serverinfo(interaction: discord.Interaction) -> None:
    guild = interaction.guild
    if not guild:
        await interaction.response.send_message("Command must be run in a guild.", ephemeral=True)
        return

    embed = discord.Embed(title=f"🏰 Server Info: {guild.name}", color=discord.Color.gold())
    if guild.icon:
        embed.set_thumbnail(url=guild.icon.url)
    embed.add_field(name="Owner", value=f"<@{guild.owner_id}>", inline=True)
    embed.add_field(name="Members", value=str(guild.member_count), inline=True)
    embed.add_field(name="Text Channels", value=str(len(guild.text_channels)), inline=True)
    embed.add_field(name="Created At", value=guild.created_at.strftime("%Y-%m-%d"), inline=True)
    await interaction.response.send_message(embed=embed)


@bot.tree.command(name="help", description="Show available commands.")
async def help_command(interaction: discord.Interaction) -> None:
    embed = discord.Embed(
        title="📖 Discord VPS Bot Manual",
        description="All commands use Discord Slash Commands (`/`).",
        color=discord.Color.blue()
    )
    embed.add_field(
        name="🖥️ VPS Commands",
        value="`/createvps` - Deploy VPS container and DM tmate SSH key\n"
              "`/deletevps` - Destroy a VPS instance\n"
              "`/listvps` - View active instances\n"
              "`/info` - Get VPS specs and tmate connection link",
        inline=False
    )
    embed.add_field(
        name="🛠️ Utilities",
        value="`/ping` - Latency check\n"
              "`/userinfo` - Get user details\n"
              "`/serverinfo` - Guild stats",
        inline=False
    )
    embed.add_field(
        name="🛡️ Moderation",
        value="`/kick` - Kick a member\n"
              "`/ban` - Ban a member\n"
              "`/purge` - Clear bulk channel messages",
        inline=False
    )
    await interaction.response.send_message(embed=embed)


# ==========================================
# MAIN ENTRYPOINT
# ==========================================
if __name__ == "__main__":
    token = config_manager.token

    if not token or token == "YOUR_DISCORD_BOT_TOKEN_HERE":
        logger.critical("ERROR: BOT TOKEN MISSING IN config.json or environment variables!")
        try:
            while True:
                time.sleep(3600)
        except KeyboardInterrupt:
            pass
    else:
        try:
            bot.run(token)
        except KeyboardInterrupt:
            logger.info("Bot execution stopped.")
        except Exception as err:
            logger.critical(f"Unhandled error: {err}\n{traceback.format_exc()}")
            try:
                while True:
                    time.sleep(3600)
            except KeyboardInterrupt:
                pass
