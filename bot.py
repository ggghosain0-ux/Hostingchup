#!/usr/bin/env python3
"""
Discord VPS Manager Bot
Production-ready Discord bot utilizing discord.py 2.5+ to manage local Docker containers as VPS environments.
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
    """Loads and validates settings from config.json safely."""
    def __init__(self, filepath: str = "config.json"):
        self.filepath = filepath
        self.config: Dict[str, Any] = {}
        self.load_config()

    def load_config(self) -> None:
        if not os.path.exists(self.filepath):
            logger.warning(f"Configuration file '{self.filepath}' was not found! Generating default template.")
            default_config = {
                "token": "YOUR_DISCORD_BOT_TOKEN_HERE",
                "guild_id": None,
                "admin_ids": [],
                "database": {"file": "database.json"},
                "docker": {
                    "image": "ubuntu:24.04",
                    "network": "bridge",
                    "base_ssh_port": 2200,
                    "container_prefix": "vps"
                },
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
                logger.error(f"Could not create default config file: {err}")
                self.config = {}
            return

        try:
            with open(self.filepath, "r", encoding="utf-8") as f:
                self.config = json.load(f)
            logger.info("Configuration file successfully loaded.")
        except json.JSONDecodeError as e:
            logger.critical(f"Config JSON corruption detected: {e}")
            self.config = {}

    def get(self, key: str, default: Any = None) -> Any:
        return self.config.get(key, default)

    @property
    def token(self) -> str:
        # Check environment variables first
        env_token = os.getenv("DISCORD_TOKEN") or os.getenv("BOT_TOKEN")
        if env_token and env_token.strip():
            return env_token.strip()

        # Check config options
        raw_token = (
            self.config.get("token") or 
            self.config.get("bot_token") or 
            self.config.get("botToken") or
            self.config.get("DISCORD_TOKEN")
        )
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
        db_val = self.config.get("database") or self.config.get("database_file")
        if isinstance(db_val, dict):
            path = db_val.get("file") or db_val.get("path") or "database.json"
            return str(path).strip()
        if isinstance(db_val, str) and db_val.strip():
            return db_val.strip()
        return "database.json"

    @property
    def server_ip(self) -> str:
        ssh_cfg = self.config.get("ssh_settings", {})
        if isinstance(ssh_cfg, dict) and "server_ip" in ssh_cfg:
            return str(ssh_cfg["server_ip"])
        return str(self.config.get("server_ip", "127.0.0.1"))

    @property
    def min_port(self) -> int:
        ssh_cfg = self.config.get("ssh_settings", {})
        if isinstance(ssh_cfg, dict) and "min_port" in ssh_cfg:
            return int(ssh_cfg["min_port"])
        docker_cfg = self.config.get("docker", {})
        if isinstance(docker_cfg, dict) and "base_ssh_port" in docker_cfg:
            return int(docker_cfg["base_ssh_port"])
        return int(self.config.get("min_port", 2200))

    @property
    def max_port(self) -> int:
        ssh_cfg = self.config.get("ssh_settings", {})
        if isinstance(ssh_cfg, dict) and "max_port" in ssh_cfg:
            return int(ssh_cfg["max_port"])
        return int(self.config.get("max_port", 30000))

    @property
    def default_ram(self) -> str:
        limits = self.config.get("limits") or self.config.get("resource_limits")
        if isinstance(limits, dict) and "default_ram" in limits:
            return str(limits["default_ram"])
        return str(self.config.get("default_ram", "512m"))

    @property
    def default_cpu(self) -> float:
        limits = self.config.get("limits") or self.config.get("resource_limits")
        if isinstance(limits, dict) and "default_cpu" in limits:
            try:
                return float(limits["default_cpu"])
            except ValueError:
                pass
        return float(self.config.get("default_cpu", 0.5))


# ==========================================
# DATABASE MANAGER
# ==========================================
class DatabaseManager:
    """Thread-safe persistent JSON flat-file database."""
    def __init__(self, filepath: str):
        self.filepath = filepath
        self.lock = asyncio.Lock()
        self._init_db()

    def _init_db(self) -> None:
        if not os.path.exists(self.filepath):
            try:
                with open(self.filepath, "w", encoding="utf-8") as f:
                    json.dump({"vps_records": {}}, f, indent=4)
                logger.info(f"Initialized database file at '{self.filepath}'.")
            except Exception as e:
                logger.error(f"Failed to create database file '{self.filepath}': {e}")

    async def _read(self) -> Dict[str, Any]:
        async with self.lock:
            if not os.path.exists(self.filepath):
                return {"vps_records": {}}
            try:
                with open(self.filepath, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    if not isinstance(data, dict):
                        return {"vps_records": {}}
                    if "vps_records" not in data:
                        data = {"vps_records": data}
                    return data
            except json.JSONDecodeError:
                logger.error("Database JSON file is corrupted! Initializing fallback memory structure.")
                return {"vps_records": {}}

    async def _write(self, data: Dict[str, Any]) -> None:
        async with self.lock:
            try:
                with open(self.filepath, "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=4)
            except Exception as e:
                logger.error(f"Failed to write to database file: {e}")

    async def get_all_vps(self) -> Dict[str, Any]:
        data = await self._read()
        return data.get("vps_records", {})

    async def get_vps(self, name: str) -> Optional[Dict[str, Any]]:
        records = await self.get_all_vps()
        return records.get(name.lower())

    async def save_vps(self, name: str, vps_data: Dict[str, Any]) -> None:
        data = await self._read()
        data["vps_records"][name.lower()] = vps_data
        await self._write(data)

    async def delete_vps(self, name: str) -> None:
        data = await self._read()
        key = name.lower()
        if key in data["vps_records"]:
            del data["vps_records"][key]
            await self._write(data)


# ==========================================
# DOCKER MANAGER
# ==========================================
class DockerManager:
    """Manages execution on the local Docker engine asynchronously."""
    def __init__(self, config_mgr: ConfigManager):
        self.config = config_mgr
        self.client: Optional[docker.DockerClient] = None
        self._connect()

    def _connect(self) -> None:
        try:
            self.client = docker.from_env()
            self.client.ping()
            logger.info("Successfully connected to Docker Daemon.")
        except Exception as e:
            logger.warning(f"Failed to connect to local Docker Daemon on startup: {e}")
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

    def find_free_port(self, currently_allocated: List[int]) -> int:
        min_p = self.config.min_port
        max_p = self.config.max_port

        for port in range(min_p, max_p + 1):
            if port in currently_allocated:
                continue
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                try:
                    s.bind(("0.0.0.0", port))
                    return port
                except OSError:
                    continue
        raise RuntimeError(f"No available ports left in range {min_p}-{max_p}.")

    async def create_container(self, name: str, ram: str, cpu: float, port: int, root_pass: str) -> str:
        def _sync_create() -> str:
            if not self.client:
                raise RuntimeError("Docker client is offline.")

            image_name = "ubuntu:24.04"
            try:
                self.client.images.get(image_name)
            except NotFound:
                logger.info(f"Base image '{image_name}' not found locally. Pulling image...")
                self.client.images.pull(image_name)

            nano_cpus = int(cpu * 1_000_000_000)

            entrypoint_cmd = (
                "/bin/bash -c "
                "\"apt-get update && apt-get install -y openssh-server && "
                "mkdir -p /var/run/sshd && "
                f"echo 'root:{root_pass}' | chpasswd && "
                "sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config && "
                "sed -i 's/#PasswordAuthentication yes/PasswordAuthentication yes/' /etc/ssh/sshd_config && "
                "/usr/sbin/sshd -D\""
            )

            container = self.client.containers.run(
                image=image_name,
                command=entrypoint_cmd,
                name=f"vps_{name}",
                detach=True,
                ports={'22/tcp': port},
                mem_limit=ram,
                nano_cpus=nano_cpus,
                restart_policy={"Name": "unless-stopped"}
            )
            return str(container.id)

        return await asyncio.to_thread(_sync_create)

    async def manage_container(self, container_id: str, action: str) -> None:
        def _sync_manage() -> None:
            if not self.client:
                raise RuntimeError("Docker client is offline.")
            try:
                container = self.client.containers.get(container_id)
                if action == "start":
                    container.start()
                elif action == "stop":
                    container.stop(timeout=10)
                elif action == "restart":
                    container.restart(timeout=10)
                elif action == "delete":
                    container.remove(force=True)
            except NotFound:
                raise ValueError("Target container was not found on Docker engine.")
            except APIError as e:
                raise RuntimeError(f"Docker API Error: {e}")

        await asyncio.to_thread(_sync_manage)

    async def get_container_status(self, container_id: str) -> str:
        def _sync_status() -> str:
            if not self.client:
                return "offline"
            try:
                container = self.client.containers.get(container_id)
                return str(container.status)
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
        super().__init__(command_prefix="!", intents=intents)
        self.cfg = cfg
        self.db = db
        self.docker = dckr

    async def setup_hook(self) -> None:
        logger.info("Executing startup command synchronization...")

        if not self.docker.is_available():
            logger.warning("Docker daemon is currently unreachable. Container management will be unavailable.")

        if self.cfg.guild_id:
            guild = discord.Object(id=self.cfg.guild_id)
            self.tree.copy_global_to(guild=guild)
            await self.tree.sync(guild=guild)
            logger.info(f"Synchronized slash command tree to Guild ID: {guild.id}")
        else:
            await self.tree.sync()
            logger.info("Synchronized slash command tree globally.")

    async def on_ready(self) -> None:
        logger.info("=" * 50)
        logger.info("VPS Bot Logged In Successfully!")
        logger.info(f"Bot User: {self.user} (ID: {self.user.id if self.user else 'Unknown'})")
        logger.info(f"Connected Guilds: {len(self.guilds)}")
        logger.info("=" * 50)

        await self.change_presence(
            activity=discord.Activity(
                type=discord.ActivityType.watching,
                name="VPS Containers"
            )
        )


# Instantiate Managers & Bot Client
config_manager = ConfigManager()
db_manager = DatabaseManager(config_manager.db_file)
docker_manager = DockerManager(config_manager)
bot = VPSBot(config_manager, db_manager, docker_manager)


# ==========================================
# ADMIN & SECURITY HELPERS
# ==========================================
def is_admin(interaction: discord.Interaction) -> bool:
    """Check if the user invoking the interaction is in admin_ids."""
    return interaction.user.id in config_manager.admin_ids


async def send_access_denied(interaction: discord.Interaction) -> None:
    embed = discord.Embed(
        title="🚫 Access Denied",
        description="You lack administrative permissions to invoke infrastructure commands.",
        color=discord.Color.red()
    )
    if interaction.response.is_done():
        await interaction.followup.send(embed=embed, ephemeral=True)
    else:
        await interaction.response.send_message(embed=embed, ephemeral=True)


def generate_secure_password(length: int = 16) -> str:
    alphabet = string.ascii_letters + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(length))


# ==========================================
# SLASH COMMANDS
# ==========================================

@bot.tree.command(name="createvps", description="Provision and launch a new virtual machine instance.")
@app_commands.describe(
    name="Unique name tag for identifying the VPS",
    vps_type="Select between Docker Container or Real Dedicated VPS",
    ram="Memory allocation (e.g. 256m, 512m, 1g)",
    cpu="CPU cores allocation (e.g. 0.5, 1.0, 2.0)"
)
@app_commands.choices(vps_type=[
    app_commands.Choice(name="Docker Container (Default)", value="docker"),
    app_commands.Choice(name="Real VPS", value="real")
])
async def createvps(
    interaction: discord.Interaction,
    name: str,
    vps_type: str = "docker",
    ram: str | None = None,
    cpu: float | None = None
) -> None:
    if not is_admin(interaction):
        await send_access_denied(interaction)
        return

    await interaction.response.defer()

    if vps_type == "real":
        cloud_api = config_manager.get("cloud_provider_api")
        if not cloud_api:
            embed = discord.Embed(
                title="🌐 Real VPS Provisioning Unavailable",
                description=(
                    "Creating a **Real VPS** with a dedicated public IPv4 address requires an external "
                    "virtualization hypervisor (e.g., Proxmox, KVM) or cloud provider API setup.\n\n"
                    "⚠️ No provider API is configured in `config.json`."
                ),
                color=discord.Color.orange()
            )
            await interaction.followup.send(embed=embed)
            return

    if not docker_manager.is_available():
        embed = discord.Embed(
            title="❌ Docker Service Unavailable",
            description="The local Docker daemon is offline or unreachable.",
            color=discord.Color.red()
        )
        await interaction.followup.send(embed=embed)
        return

    clean_name = "".join(c for c in name if c.isalnum() or c in ('-', '_')).lower()
    if not clean_name:
        embed = discord.Embed(
            title="❌ Invalid Name",
            description="Please provide a valid alphanumeric VPS name.",
            color=discord.Color.red()
        )
        await interaction.followup.send(embed=embed)
        return

    existing = await db_manager.get_vps(clean_name)
    if existing:
        embed = discord.Embed(
            title="❌ Name Collision",
            description=f"A VPS named `{clean_name}` already exists.",
            color=discord.Color.red()
        )
        await interaction.followup.send(embed=embed)
        return

    all_vps = await db_manager.get_all_vps()
    allocated_ports = [
        int(rec["ssh_port"]) for rec in all_vps.values() if isinstance(rec, dict) and "ssh_port" in rec
    ]

    try:
        ssh_port = docker_manager.find_free_port(allocated_ports)
    except Exception as err:
        embed = discord.Embed(
            title="❌ Network Port Allocation Failed",
            description=str(err),
            color=discord.Color.red()
        )
        await interaction.followup.send(embed=embed)
        return

    allocated_ram = ram if ram else config_manager.default_ram
    allocated_cpu = cpu if cpu is not None else config_manager.default_cpu
    root_pass = generate_secure_password(16)

    try:
        container_id = await docker_manager.create_container(
            name=clean_name,
            ram=allocated_ram,
            cpu=allocated_cpu,
            port=ssh_port,
            root_pass=root_pass
        )

        vps_record = {
            "owner_id": interaction.user.id,
            "vps_name": clean_name,
            "container_id": container_id,
            "creation_time": datetime.now(timezone.utc).isoformat(),
            "ram_limit": allocated_ram,
            "cpu_limit": allocated_cpu,
            "ssh_port": ssh_port,
            "password": root_pass,
            "status": "running"
        }

        await db_manager.save_vps(clean_name, vps_record)

        embed = discord.Embed(
            title="🚀 VPS Provisioned Successfully",
            description=f"Container instance `{clean_name}` is active.",
            color=discord.Color.green()
     )
        embed.add_field(name="VPS Name", value=clean_name, inline=True)
        embed.add_field(name="Container ID", value=container_id[:12], inline=True)
        embed.add_field(name="RAM Limit", value=allocated_ram, inline=True)
        embed.add_field(name="CPU Cores", value=str(allocated_cpu), inline=True)
        embed.add_field(name="SSH Port", value=str(ssh_port), inline=True)
        embed.add_field(name="Root Password", value=f"||{root_pass}||", inline=False)
        embed.add_field(
            name="💻 Connection String",
            value=f"```bash\nssh root@{config_manager.server_ip} -p {ssh_port}\n```",
            inline=False
        )
        await interaction.followup.send(embed=embed)

    except Exception as e:
        logger.exception("Failed to create container.")
        embed = discord.Embed(
            title="❌ Deployment Error",
            description=f"Failed to provision VPS: ```py\n{e}\n```",
            color=discord.Color.red()
        )
        await interaction.followup.send(embed=embed)


@bot.tree.command(name="deletevps", description="Delete an existing VPS instance.")
@app_commands.describe(name="Name of the VPS to delete")
async def deletevps(interaction: discord.Interaction, name: str) -> None:
    if not is_admin(interaction):
        await send_access_denied(interaction)
        return

    await interaction.response.defer()
    clean_name = name.lower()
    vps_data = await db_manager.get_vps(clean_name)

    if not vps_data:
        embed = discord.Embed(
            title="❌ VPS Not Found",
            description=f"No instance named `{clean_name}` exists in the database.",
            color=discord.Color.red()
        )
        await interaction.followup.send(embed=embed)
        return

    try:
        if docker_manager.is_available():
            await docker_manager.manage_container(vps_data["container_id"], "delete")

        await db_manager.delete_vps(clean_name)
        embed = discord.Embed(
            title="🗑️ VPS Deleted",
            description=f"The VPS `{clean_name}` and its resources were permanently removed.",
            color=discord.Color.green()
        )
        await interaction.followup.send(embed=embed)
    except Exception as e:
        logger.exception("Deletion error")
        await db_manager.delete_vps(clean_name)
        embed = discord.Embed(
            title="⚠️ Partial Deletion Warning",
            description=f"Removed record from database, but Docker reported: `{e}`",
            color=discord.Color.orange()
        )
        await interaction.followup.send(embed=embed)


@bot.tree.command(name="listvps", description="List all deployed VPS instances.")
async def listvps(interaction: discord.Interaction) -> None:
    await interaction.response.defer()
    records = await db_manager.get_all_vps()

    if not records:
        embed = discord.Embed(
            title="📋 VPS Inventory",
            description="No VPS instances found in the database.",
            color=discord.Color.blue()
        )
        await interaction.followup.send(embed=embed)
        return

    embed = discord.Embed(title="📋 Active VPS Inventory", color=discord.Color.blue())

    for name, data in records.items():
        if not isinstance(data, dict):
            continue
        status = "UNKNOWN"
        if docker_manager.is_available() and "container_id" in data:
            status = await docker_manager.get_container_status(data["container_id"])

        details = (
            f"**Container ID:** `{data.get('container_id', 'N/A')[:12]}`\n"
            f"**Owner:** <@{data.get('owner_id')}>\n"
            f"**Limits:** CPU `{data.get('cpu_limit')}` | RAM `{data.get('ram_limit')}`\n"
            f"**SSH Port:** `{data.get('ssh_port')}`\n"
            f"**Status:** `{status.upper()}`"
        )
        embed.add_field(name=f"🖥️ {name}", value=details, inline=False)

    await interaction.followup.send(embed=embed)


@bot.tree.command(name="startvps", description="Start a stopped VPS instance.")
@app_commands.describe(name="Name of the VPS to start")
async def startvps(interaction: discord.Interaction, name: str) -> None:
    if not is_admin(interaction):
        await send_access_denied(interaction)
        return

    await interaction.response.defer()
    clean_name = name.lower()
    vps_data = await db_manager.get_vps(clean_name)

    if not vps_data:
        embed = discord.Embed(title="❌ Error", description="VPS not found.", color=discord.Color.red())
        await interaction.followup.send(embed=embed)
        return

    try:
        await docker_manager.manage_container(vps_data["container_id"], "start")
        vps_data["status"] = "running"
        await db_manager.save_vps(clean_name, vps_data)
        embed = discord.Embed(
            title="🟩 VPS Started",
            description=f"VPS `{clean_name}` is now running.",
            color=discord.Color.green()
        )
        await interaction.followup.send(embed=embed)
    except Exception as e:
        embed = discord.Embed(title="❌ Action Failed", description=str(e), color=discord.Color.red())
        await interaction.followup.send(embed=embed)


@bot.tree.command(name="stopvps", description="Stop a running VPS instance.")
@app_commands.describe(name="Name of the VPS to stop")
async def stopvps(interaction: discord.Interaction, name: str) -> None:
    if not is_admin(interaction):
        await send_access_denied(interaction)
        return

    await interaction.response.defer()
    clean_name = name.lower()
    vps_data = await db_manager.get_vps(clean_name)

    if not vps_data:
        embed = discord.Embed(title="❌ Error", description="VPS not found.", color=discord.Color.red())
        await interaction.followup.send(embed=embed)
        return

    try:
        await docker_manager.manage_container(vps_data["container_id"], "stop")
        vps_data["status"] = "stopped"
        await db_manager.save_vps(clean_name, vps_data)
        embed = discord.Embed(
            title="🟥 VPS Stopped",
            description=f"VPS `{clean_name}` has been stopped.",
            color=discord.Color.green()
        )
        await interaction.followup.send(embed=embed)
    except Exception as e:
        embed = discord.Embed(title="❌ Action Failed", description=str(e), color=discord.Color.red())
        await interaction.followup.send(embed=embed)


@bot.tree.command(name="restartvps", description="Restart a VPS instance.")
@app_commands.describe(name="Name of the VPS to restart")
async def restartvps(interaction: discord.Interaction, name: str) -> None:
    if not is_admin(interaction):
        await send_access_denied(interaction)
        return

    await interaction.response.defer()
    clean_name = name.lower()
    vps_data = await db_manager.get_vps(clean_name)

    if not vps_data:
        embed = discord.Embed(title="❌ Error", description="VPS not found.", color=discord.Color.red())
        await interaction.followup.send(embed=embed)
        return

    try:
        await docker_manager.manage_container(vps_data["container_id"], "restart")
        vps_data["status"] = "running"
        await db_manager.save_vps(clean_name, vps_data)
        embed = discord.Embed(
            title="🟨 VPS Restarted",
            description=f"VPS `{clean_name}` has been restarted.",
            color=discord.Color.green()
        )
        await interaction.followup.send(embed=embed)
    except Exception as e:
        embed = discord.Embed(title="❌ Action Failed", description=str(e), color=discord.Color.red())
        await interaction.followup.send(embed=embed)


@bot.tree.command(name="info", description="View detailed information about a VPS.")
@app_commands.describe(name="Name of the VPS instance")
async def info(interaction: discord.Interaction, name: str) -> None:
    clean_name = name.lower()
    vps_data = await db_manager.get_vps(clean_name)

    if not vps_data:
        embed = discord.Embed(
            title="❌ Not Found",
            description=f"No VPS named `{clean_name}` was found.",
            color=discord.Color.red()
        )
        await interaction.response.send_message(embed=embed)
        return

    if not is_admin(interaction) and interaction.user.id != vps_data.get("owner_id"):
        await send_access_denied(interaction)
        return

    status = "UNKNOWN"
    if docker_manager.is_available() and "container_id" in vps_data:
        status = await docker_manager.get_container_status(vps_data["container_id"])

    embed = discord.Embed(title=f"🛠️ Details for VPS: {clean_name}", color=discord.Color.purple())
    embed.add_field(name="Owner", value=f"<@{vps_data.get('owner_id')}>", inline=True)
    embed.add_field(name="Status", value=status.upper(), inline=True)
    embed.add_field(name="Created At", value=str(vps_data.get("creation_time", "N/A")), inline=False)
    embed.add_field(name="CPU Limit", value=str(vps_data.get("cpu_limit")), inline=True)
    embed.add_field(name="RAM Limit", value=str(vps_data.get("ram_limit")), inline=True)
    embed.add_field(name="SSH Port", value=str(vps_data.get("ssh_port")), inline=True)
    embed.add_field(name="Root Password", value=f"||{vps_data.get('password')}||", inline=False)
    embed.add_field(
        name="SSH Command",
        value=f"```bash\nssh root@{config_manager.server_ip} -p {vps_data.get('ssh_port')}\n```",
        inline=False
    )

    await interaction.response.send_message(embed=embed)


@bot.tree.command(name="stats", description="View host system and Docker engine metrics.")
async def stats(interaction: discord.Interaction) -> None:
    await interaction.response.defer()

    if not docker_manager.is_available():
        embed = discord.Embed(
            title="❌ System Status",
            description="Docker Daemon is offline or unreachable.",
            color=discord.Color.red()
        )
        await interaction.followup.send(embed=embed)
        return

    try:
        all_records = await db_manager.get_all_vps()
        total_tracked = len(all_records)

        containers = await asyncio.to_thread(
            lambda: docker_manager.client.containers.list(all=True) if docker_manager.client else []
        )
        running_cnt = sum(1 for c in containers if c.status == "running")

        embed = discord.Embed(title="📊 Host System & Docker Status", color=discord.Color.gold())
        embed.add_field(name="Docker Daemon", value="🟢 ONLINE", inline=True)
        embed.add_field(name="Tracked Database Records", value=str(total_tracked), inline=True)
        embed.add_field(name="Active Containers", value=str(running_cnt), inline=True)
        embed.add_field(name="Server IP", value=config_manager.server_ip, inline=True)
        embed.add_field(
            name="Port Allocation Range",
            value=f"{config_manager.min_port} - {config_manager.max_port}",
            inline=True
        )

        await interaction.followup.send(embed=embed)
    except Exception as e:
        embed = discord.Embed(
            title="❌ Error Fetching Metrics",
            description=str(e),
            color=discord.Color.red()
        )
        await interaction.followup.send(embed=embed)


@bot.tree.command(name="help", description="Display the command manual.")
async def help_command(interaction: discord.Interaction) -> None:
    embed = discord.Embed(
        title="📖 Discord VPS Manager Manual",
        description="Manage lightweight local Docker instances via Slash Commands.",
        color=discord.Color.blue()
    )

    user_cmds = (
        "`/listvps` - Show all deployed instances.\n"
        "`/info <name>` - View server info and login credentials.\n"
        "`/stats` - Display host Docker daemon status.\n"
        "`/help` - Show this command list."
    )
    embed.add_field(name="👥 General Commands", value=user_cmds, inline=False)

    admin_cmds = (
        "`/createvps` - Deploy a new VPS instance.\n"
        "`/deletevps <name>` - Remove a VPS instance and wipe data.\n"
        "`/startvps <name>` - Start a stopped instance.\n"
        "`/stopvps <name>` - Stop an active instance.\n"
        "`/restartvps <name>` - Power cycle an instance."
    )
    embed.add_field(name="🛡️ Administrator Commands", value=admin_cmds, inline=False)

    await interaction.response.send_message(embed=embed)


# ==========================================
# MAIN APPLICATION ENTRYPOINT
# ==========================================
if __name__ == "__main__":
    token = config_manager.token

    if not token or token == "YOUR_DISCORD_BOT_TOKEN_HERE":
        logger.critical("=" * 60)
        logger.critical("ERROR: DISCORD BOT TOKEN IS MISSING OR INVALID IN config.json!")
        logger.critical("Please open config.json and enter a valid Discord Bot Token.")
        logger.critical("=" * 60)
        logger.info("Keeping terminal process active. Press Ctrl+C to exit.")
        try:
            while True:
                time.sleep(3600)
        except KeyboardInterrupt:
            logger.info("Process stopped manually.")
    else:
        try:
            logger.info("Starting Discord Client connection...")
            bot.run(token)
        except discord.errors.LoginFailure as err:
            logger.critical("=" * 60)
            logger.critical(f"DISCORD LOGIN FAILED: Invalid Token provided! {err}")
            logger.critical("Please update config.json with a valid token.")
            logger.critical("=" * 60)
            logger.info("Keeping process alive for debugging. Press Ctrl+C to stop.")
            try:
                while True:
                    time.sleep(3600)
            except KeyboardInterrupt:
                logger.info("Process stopped manually.")
        except KeyboardInterrupt:
            logger.info("Bot execution stopped by user (Ctrl+C).")
        except Exception as err:
            logger.critical(f"Unhandled runtime error: {err}")
            logger.critical(traceback.format_exc())
            logger.info("Keeping process alive. Press Ctrl+C to stop.")
            try:
                while True:
                    time.sleep(3600)
            except KeyboardInterrupt:
                logger.info("Process stopped manually.") 
