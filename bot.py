#!/usr/bin/env python3
"""
Discord VPS Manager Bot
Production-ready Discord bot utilizing discord.py 2.5+ to manage local Docker containers as VPS environments.
"""

import os
import sys
import json
import socket
import string
import secrets
import logging
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
            msg = f"Configuration file '{self.filepath}' was not found!"
            logger.critical(msg)
            raise FileNotFoundError(msg)

        try:
            with open(self.filepath, "r", encoding="utf-8") as f:
                self.config = json.load(f)
            logger.info("Configuration file successfully loaded.")
        except json.JSONDecodeError as e:
            logger.critical(f"Config JSON corruption detected: {e}")
            raise

    def get(self, key: str, default: Any = None) -> Any:
        return self.config.get(key, default)

    @property
    def token(self) -> str:
        token_val = self.config.get("bot_token") or self.config.get("token")
        if not token_val:
            raise KeyError("Neither 'bot_token' nor 'token' found in config.json")
        return str(token_val)

    @property
    def guild_id(self) -> Optional[int]:
        gid = self.config.get("guild_id")
        return int(gid) if gid else None

    @property
    def admin_ids(self) -> List[int]:
        raw_ids = self.config.get("admin_ids", [])
        return [int(uid) for uid in raw_ids if str(uid).isdigit()]

    @property
    def db_file(self) -> str:
        return str(self.config.get("database_file") or self.config.get("database") or "database.json")

    @property
    def server_ip(self) -> str:
        ssh_cfg = self.config.get("ssh_settings", {})
        return str(ssh_cfg.get("server_ip", "127.0.0.1"))

    @property
    def min_port(self) -> int:
        ssh_cfg = self.config.get("ssh_settings", {})
        return int(ssh_cfg.get("min_port", 20000))

    @property
    def max_port(self) -> int:
        ssh_cfg = self.config.get("ssh_settings", {})
        return int(ssh_cfg.get("max_port", 30000))

    @property
    def default_ram(self) -> str:
        res_cfg = self.config.get("resource_limits", {})
        return str(res_cfg.get("default_ram", "512m"))

    @property
    def default_cpu(self) -> float:
        res_cfg = self.config.get("resource_limits", {})
        return float(res_cfg.get("default_cpu", 0.5))


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
            with open(self.filepath, "w", encoding="utf-8") as f:
                json.dump({"vps_records": {}}, f, indent=4)
            logger.info(f"Initialized database file at '{self.filepath}'.")

    async def _read(self) -> Dict[str, Any]:
        async with self.lock:
            if not os.path.exists(self.filepath):
                return {"vps_records": {}}
            try:
                with open(self.filepath, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    if "vps_records" not in data:
                        data["vps_records"] = {}
                    return data
            except json.JSONDecodeError:
                logger.error("Database JSON file is corrupted! Initializing fallback structure.")
                return {"vps_records": {}}

    async def _write(self, data: Dict[str, Any]) -> None:
        async with self.lock:
            with open(self.filepath, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=4)

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

            # Execution script to set up SSH daemon cleanly inside standard Ubuntu image
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
        logger.info("Performing startup diagnostics...")

        if not self.docker.is_available():
            logger.warning("Docker daemon is unreachable. Local VPS functionality will be limited.")

        if self.cfg.guild_id:
            guild = discord.Object(id=self.cfg.guild_id)
            self.tree.copy_global_to(guild=guild)
            await self.tree.sync(guild=guild)
            logger.info(f"Synchronized slash command tree to Guild ID: {guild.id}")
        else:
            await self.tree.sync()
            logger.info("Synchronized slash command tree globally.")

    async def on_ready(self) -> None:
        logger.info(f"Bot logged in as {self.user} (ID: {self.user.id if self.user else 'Unknown'})")
        await self.change_presence(
            activity=discord.Activity(
                type=discord.ActivityType.watching,
                name="VPS Containers"
            )
        )


# Instantiate Global Services
config_manager = ConfigManager()
db_manager = DatabaseManager(config_manager.db_file)
docker_manager = DockerManager(config_manager)
bot = VPSBot(config_manager, db_manager, docker_manager)


# ==========================================
# ADMIN HELPER
# ==========================================
def is_admin(interaction: discord.Interaction) -> bool:
    """Check if the interacting user is listed in admin_ids."""
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
                    "⚠️ No provider API is configured in `config.json`. The bot will not fabricate fake IP addresses."
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
        int(rec["ssh_port"]) for rec in all_vps.values() if "ssh_port" in rec
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

    embed = discord.Em
