#!/usr/bin/env python3
"""
Discord VPS Manager Bot
A production-ready Discord bot utilizing discord.py 2.x to manage local Docker containers as lightweight VPS environments.
"""

import os
import json
import socket
import secrets
import logging
import asyncio
import datetime
from typing import Dict, Any, List, Optional, Tuple

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
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("VPSManagerBot")


# ==========================================
# CONFIGURATION & DATABASE MANAGERS
# ==========================================
class ConfigManager:
    """Handles loading and validating settings from config.json."""
    def __init__(self, filepath: str = "config.json"):
        self.filepath = filepath
        self.config: Dict[str, Any] = {}
        self.load_config()

    def load_config(self):
        if not os.path.exists(self.filepath):
            critical_error = f"Configuration file '{self.filepath}' is missing! Cannot start bot."
            logger.critical(critical_error)
            raise FileNotFoundError(critical_error)
        
        try:
            with open(self.filepath, "r", encoding="utf-8") as f:
                self.config = json.load(f)
            logger.info("Configuration successfully loaded.")
        except json.JSONDecodeError as e:
            logger.critical(f"Config JSON Corruption: {e}")
            raise

    def get(self, key: str, default: Any = None) -> Any:
        return self.config.get(key, default)

    @property
    def token(self) -> str:
        return self.config["bot_token"]

    @property
    def guild_id(self) -> Optional[int]:
        gid = self.config.get("guild_id")
        return int(gid) if gid else None

    @property
    def admin_ids(self) -> List[int]:
        return [int(uid) for uid in self.config.get("admin_ids", [])]

    @property
    def db_file(self) -> str:
        return self.config.get("database_file", "vps_database.json")

    @property
    def server_ip(self) -> str:
        return self.config.get("ssh_settings", {}).get("server_ip", "127.0.0.1")

    @property
    def min_port(self) -> int:
        return self.config.get("ssh_settings", {}).get("min_port", 20000)

    @property
    def max_port(self) -> int:
        return self.config.get("ssh_settings", {}).get("max_port", 30000)

    @property
    def default_ram(self) -> str:
        return self.config.get("resource_limits", {}).get("default_ram", "512m")

    @property
    def default_cpu(self) -> float:
        return float(self.config.get("resource_limits", {}).get("default_cpu", 0.5))


class DatabaseManager:
    """Thread-safe and exception-resistant persistent JSON Flat-file database layer."""
    def __init__(self, filepath: str):
        self.filepath = filepath
        self.lock = asyncio.Lock()
        self._init_db()

    def _init_db(self):
        if not os.path.exists(self.filepath):
            with open(self.filepath, "w", encoding="utf-8") as f:
                json.dump({"vps_records": {}}, f, indent=4)
            logger.info(file_msg := f"Created database file at {self.filepath}")

    async def _read(self) -> Dict[str, Any]:
        async with self.lock:
            try:
                with open(self.filepath, "r", encoding="utf-8") as f:
                    return json.load(f)
            except json.JSONDecodeError:
                logger.error("Database JSON file corrupted! Attempting recovery from backup or re-initializing.")
                return {"vps_records": {}}

    async def _write(self, data: Dict[str, Any]):
        async with self.lock:
            with open(self.filepath, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=4)

    async def get_all_vps(self) -> Dict[str, Any]:
        data = await self._read()
        return data.get("vps_records", {})

    async def get_vps(self, name: str) -> Optional[Dict[str, Any]]:
        records = await self.get_all_vps()
        return records.get(name)

    async def save_vps(self, name: str, vps_data: Dict[str, Any]):
        data = await self._read()
        data["vps_records"][name] = vps_data
        await self._write(data)

    async def delete_vps(self, name: str):
        data = await self._read()
        if name in data["vps_records"]:
            del data["vps_records"][name]
            await self._write(data)


# ==========================================
# DOCKER ORCHESTRATION LAYER
# ==========================================
class DockerManager:
    """Manages secure execution operations on the local Docker engine asynchronously."""
    def __init__(self, config_mgr: ConfigManager):
        self.config = config_mgr
        try:
            self.client = docker.from_env()
            self.client.ping()
        except DockerException as e:
            logger.critical(f"Cannot interface with local Docker Engine daemon: {e}")
            self.client = None

    def is_available(self) -> bool:
        if self.client is None:
            return False
        try:
            return self.client.ping()
        except DockerException:
            return False

    def find_free_port(self, current_allocated: list) -> int:
        min_p = self.config.min_port
        max_p = self.config.max_port
        
        for port in range(min_p, max_p + 1):
            if port in current_allocated:
                continue
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                try:
                    s.bind(("0.0.0.0", port))
                    return port
                except OSError:
                    continue
        raise IOError("No unallocated or open SSH ports left within configured range boundaries.")

    async def create_container(self, name: str, ram: str, cpu: float, port: int, root_pass: str) -> str:
        """Asynchronously dispatches heavy container setups to executor threads."""
        def _run():
            # Construct automated systemd / openssh entrypoint payload
            # Installs OpenSSH Server and sets up password authenticated root access dynamically on standard Ubuntu image
            image_name = "ubuntu:24.04"
            try:
                self.client.images.get(image_name)
            except NotFound:
                logger.info(f"Pulling base image: {image_name}")
                self.client.images.pull(image_name)

            # Nano-CPUs calculations: 1 CPU = 1,000,000,000 nano-cpus
            nano_cpus = int(cpu * 1000000000)

            # Execution sequence command to configure SSH natively without prebuilt custom image dependencies
            entrypoint_cmd = (
                f"/bin/bash -c '"
                f"apt-get update && apt-get install -y openssh-server && "
                f"mkdir /var/run/sshd && "
                f"echo \"root:{root_pass}\" | chpasswd && "
                f"sed -i \"s/#PermitRootLogin prohibit-password/PermitRootLogin yes/\" /etc/ssh/sshd_config && "
                f"sed -i \"s/#PasswordAuthentication yes/PasswordAuthentication yes/\" /etc/ssh/sshd_config && "
                f"/usr/sbin/sshd -D'"
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
            return container.id

        return await asyncio.to_thread(_run)

    async def manage_container(self, container_id: str, action: str):
        def _action():
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
                raise ValueError("Container not present inside local virtualization engine.")
            except APIError as e:
                raise RuntimeError(f"Docker API Exception: {e}")

        await asyncio.to_thread(_action)

    async def get_container_status(self, container_id: str) -> str:
        def _status():
            try:
                container = self.client.containers.get(container_id)
                return container.status
            except NotFound:
                return "missing"
        return await asyncio.to_thread(_status)


# ==========================================
# DISCORD BOT INFRASTRUCTURE & APPLICATION
# ==========================================
class VPSManagerBot(commands.Bot):
    def __init__(self, config_mgr: ConfigManager, db_mgr: DatabaseManager, docker_mgr: DockerManager):
        intents = discord.Intents.default()
        super().__init__(command_prefix="!", intents=intents)
        self.config_mgr = config_mgr
        self.db_mgr = db_mgr
        self.docker_mgr = docker_mgr

    async def setup_hook(self):
        # Startup infrastructure integrity checks
        logger.info("Executing operational health system checks...")
        if not self.docker_mgr.is_available():
            logger.critical("Initialization Failed: Docker is completely offline or unavailable!")
        else:
            logger.info("Docker daemon status check: ONLINE.")

        # Sync app commands for defined execution guild scope or globally
        if self.config_mgr.guild_id:
            guild = discord.Object(id=self.config_mgr.guild_id)
            self.tree.copy_global_to(guild=guild)
            await self.tree.sync(guild=guild)
            logger.info(f"Command application trees synchronized target guild: {guild.id}")
        else:
            await self.tree.sync()
            logger.info("Command application trees synchronized globally.")

    async def on_ready(self):
        logger.info(f"Bot system active. Authenticated network interface as user: {self.user}")
        await self.change_presence(activity=discord.Activity(type=discord.ActivityType.watching, name="Local VPS Instances"))


# Initialize dependencies globally
config_manager = ConfigManager()
db_manager = DatabaseManager(config_manager.db_file)
docker_manager = DockerManager(config_manager)
bot = VPSManagerBot(config_manager, db_manager, docker_manager)


# ==========================================
# PERMISSION AND SECURITY DECORATORS
# ==========================================
def is_admin_check(interaction: discord.Interaction) -> bool:
    return interaction.user.id in config_manager.admin_ids

def admin_only():
    def decorator(func):
        async def wrapper(interaction: discord.Interaction, *args, **kwargs):
            if not is_admin_check(interaction):
                embed = discord.Embed(
                    title="🚫 Access Denied",
                    description="You lack administrative permissions to invoke infrastructure core management components.",
                    color=discord.Color.red()
                )
                await interaction.response.send_message(embed=embed, ephemeral=True)
                return
            return await func(interaction, *args, **kwargs)
        return wrapper
    return decorator


# ==========================================
# APPLICATION INTERACTION SLASH COMMANDS
# ==========================================
@bot.tree.command(name="createvps", description="Provision and launch a new virtual isolated machine instance.")
@app_commands.describe(
    name="Unique name tag for identifying the VPS container mapping",
    vps_type="Select between Docker Container virtualization or Dedicated Bare-Metal Virtualization",
    ram="Memory limit allocation profile string (ex: 256m, 512m, 1g)",
    cpu="Core processing units allocation fraction (ex: 0.5, 1.0, 2.0)"
)
@app_commands.choices(vps_type=[
    app_commands.Choice(name="Docker Container (Default)", value="docker"),
    app_commands.Choice(name="Real Dedicated VPS", value="real")
])
@admin_only()
def create_vps(
    interaction: discord.Interaction,
    name: str,
    vps_type: str = "docker",
    ram: Optional[str] = None,
    cpu: Optional[float] = None
):
    async def _execute():
        await interaction.response.defer(ephemeral=False)
        
        # Guard clause: Verify Docker Engine State
        if not docker_manager.is_available():
            embed = discord.Embed(
                title="❌ Cluster System Error",
                description="The local Docker orchestration engine is currently unreachable. Cannot handle provisioning tasks.",
                color=discord.Color.red()
            )
            await interaction.followup.send(embed=embed)
            return

        # Sanitize name
        clean_name = "".join(c for c in name if c.isalnum() or c in ('-', '_')).lower()
        if not clean_name:
            embed = discord.Embed(title="❌ Creation Aborted", description="Invalid alphanumeric string name provided.", color=discord.Color.red())
            await interaction.followup.send(embed=embed)
            return

        # Check for Duplicate VPS Names
        existing_records = await db_manager.get_all_vps()
        if clean_name in existing_records:
            embed = discord.Embed(
                title="❌ Namespace Collision",
                description=f"A virtual server named `{clean_name}` already exists inside configuration clusters.",
                color=discord.Color.red()
            )
            await interaction.followup.send(embed=embed)
            return

        # Rule evaluation for API Provisioners configurations
        if vps_type == "real":
            embed = discord.Embed(
                title="🌐 Provisioning External Infrastructure Blocked",
                description=(
                    "You requested an isolated physical **Real VPS** with a dedicated public IPv4 resource wrapper.\n\n"
                    "⚠️ **Error:** No hypervisor virtualization platform backend (KVM/Proxmox) or Cloud Provider API tokens (AWS, DigitalOcean) "
                    "have been initialized inside `config.json` configuration structures.\n\n"
                    "*The bot cannot fabricate dedicated hardware clusters out of pure air. Only local containers can be safely mapped.*"
                ),
                color=discord.Color.orange()
            )
            await interaction.followup.send(embed=embed)
            return

        # Parse thresholds resources
        allocated_ram = ram if ram else config_manager.default_ram
        allocated_cpu = cpu if cpu else config_manager.default_cpu

        # Collect current ports mappings to prevent collision structures
        used_ports = [record.get("ssh_port") for record in existing_records.values() if record.get("ssh_port")]
        
        try:
            free_port = docker_manager.find_free_port(used_ports)
        except IOError as err:
            embed = discord.Embed(title="❌ Network Allocation Exhausted", description=str(err), color=discord.Color.red())
            await interaction.followup.send(embed=embed)
            return

        generated_pass = secrets.token_urlsafe(12)

        try:
            container_id = await docker_manager.create_container(
                name=clean_name,
                ram=allocated_ram,
                cpu=allocated_cpu,
                port=free_port,
                root_pass=generated_pass
            )

            # Record structure object
            vps_payload = {
                "owner_id": interaction.user.id,
                "vps_name": clean_name,
                "container_id": container_id,
                "creation_time": datetime.datetime.utcnow().isoformat(),
                "ram_limit": allocated_ram,
                "cpu_limit": allocated_cpu,
                "ssh_port": free_port,
                "password": generated_pass,
                "status": "running"
            }
            
            await db_manager.save_vps(clean_name, vps_payload)

            embed = discord.Embed(
                title="🚀 VPS Infrastructure Provisioned Successfully",
                description=f"Container system for `{clean_name}` has been successfully configured and deployed locally.",
                color=discord.Color.green()
            )
            embed.add_field(name="Instance Identification Label", value=clean_name, inline=True)
            embed.add_field(name="Container Runtime ID", value=container_id[:12], inline=True)
            embed.add_field(name="Core Memory Max Profile", value=allocated_ram, inline=True)
            embed.add_field(name="CPU Units Limit", value=str(allocated_cpu), inline=True)
            embed.add_field(name="Network Mapped Port", value=str(free_port), inline=True)
            embed.add_field(name="Secure Root Password", value=f"||{generated_pass}||", inline=False)
            embed.add_field(
                name="💻 Direct Shell Access String Command",
                value=f"```bash\nssh root@{config_manager.server_ip} -p {free_port}\n```",
                inline=False
            )
            embed.set_footer(text="Notice: Standard initialization requires ~5-15s for internal SSH packages down-streaming installation.")
            await interaction.followup.send(embed=embed)

        except Exception as e:
            logger.exception("Error while creating instance")
            embed = discord.Embed(
                title="❌ Failure During Deploy Routine",
                description=f"An unhandled backend architecture exception occurred: ```py\n{e}\n```",
                color=discord.Color.red()
            )
            await interaction.followup.send(embed=embed)

    asyncio.create_task(_execute())


@bot.tree.command(name="deletevps", description="Purge and securely erase a defined container instance mapping.")
@app_commands.describe(name="Target unique tracking label name of the instance to delete")
@admin_only()
def delete_vps(interaction: discord.Interaction, name: str):
    async def _execute():
        await interaction.response.defer()
        clean_name = name.lower()
        vps_data = await db_manager.get_vps(clean_name)

        if not vps_data:
            embed = discord.Embed(title="❌ Instance Registry Missing", description=f"Could not locate an instance named `{clean_name}`.", color=discord.Color.red())
            await interaction.followup.send(embed=embed)
            return

        try:
            if docker_manager.is_available():
                await docker_manager.manage_container(vps_data["container_id"], "delete")
            
            await db_manager.delete_vps(clean_name)
            embed = discord.Embed(
                title="🗑️ Infrastructure Purged",
                description=f"The instance registry and virtualization container environment for `{clean_name}` was wiped out permanently.",
                color=discord.Color.green()
            )
            await interaction.followup.send(embed=embed)
        except Exception as e:
            logger.exception("Deletion error")
            await db_manager.delete_vps(clean_name)
            embed = discord.Embed(
                title="⚠️ Partial Infrastructure Removal Alert",
                description=f"Cleaned configuration keys from disk, but underlying engine threw error during drop routines: `{e}`",
                color=discord.Color.orange()
