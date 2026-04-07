import asyncio
import logging
import os
import time
from decimal import Decimal, InvalidOperation

import discord
from discord import app_commands
from dotenv import load_dotenv

import db

load_dotenv()

BOT_TOKEN = os.environ["BOT_TOKEN"]
GUILD_ID = int(os.environ["GUILD_ID"])

ZERO_VOTE_DURATION = 60  # seconds
ZERO_COOLDOWN = 86400  # 24 hours

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("point-counter")

intents = discord.Intents.default()
client = discord.Client(intents=intents)
tree = app_commands.CommandTree(client)

guild_obj = discord.Object(id=GUILD_ID)


MAX_DISPLAY_LEN = 100  # Discord-safe length for a single number


def _fmt_points(val: Decimal) -> str:
    """Format a Decimal: as integer if whole, full precision if it fits, scientific notation if too long."""
    if val == val.to_integral_value():
        return str(int(val))
    text = format(val.normalize(), 'f')
    if len(text) > MAX_DISPLAY_LEN:
        return f"{val.normalize():E} *(sci. notation — too long for Discord)*"
    return text


@tree.error
async def on_app_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    log.error("Command error: %s", error, exc_info=error)
    msg = "Something went wrong. Please try again."
    try:
        if interaction.response.is_done():
            await interaction.followup.send(msg, ephemeral=True)
        else:
            await interaction.response.send_message(msg, ephemeral=True)
    except discord.HTTPException:
        pass


async def _handle_zero_vote(
    interaction: discord.Interaction,
    user: discord.Member,
    guild_id: str,
    reason: str | None,
):
    """Start a vote to zero out a user's points."""
    from_user = interaction.user

    # Check 24h cooldown
    last_zero = db.get_last_zero_time(guild_id, str(from_user.id))
    if last_zero is not None:
        elapsed = time.time() - last_zero
        if elapsed < ZERO_COOLDOWN:
            next_available = int(last_zero + ZERO_COOLDOWN)
            await interaction.followup.send(
                f"You can only zero someone once every 24 hours. "
                f"Try again <t:{next_available}:R>.",
                ephemeral=True,
            )
            return

    current = db.get_user_points(guild_id, str(user.id))
    if current == 0:
        await interaction.followup.send(
            f"{user.mention} already has **0** points.", ephemeral=True
        )
        return

    deadline = int(time.time()) + ZERO_VOTE_DURATION

    embed = discord.Embed(
        title="Zero Vote",
        description=(
            f"Does {user.mention} deserve to get zeroed?\n"
            f"You have <t:{deadline}:R> to vote."
        ),
        color=discord.Color.red(),
    )
    if reason:
        embed.add_field(name="Reason", value=reason, inline=False)
    embed.set_footer(text=f"Called by {from_user.display_name}")

    vote_msg = await interaction.followup.send(embed=embed, wait=True)
    await vote_msg.add_reaction("\U0001f44d")
    await vote_msg.add_reaction("\U0001f44e")

    async def tally():
        await asyncio.sleep(ZERO_VOTE_DURATION)
        try:
            vote_msg_updated = await interaction.channel.fetch_message(vote_msg.id)
        except discord.HTTPException:
            log.error("Could not fetch zero vote message")
            return

        thumbs_up = 0
        thumbs_down = 0
        for reaction in vote_msg_updated.reactions:
            if str(reaction.emoji) == "\U0001f44d":
                thumbs_up = reaction.count - 1  # subtract bot's own reaction
            elif str(reaction.emoji) == "\U0001f44e":
                thumbs_down = reaction.count - 1

        if thumbs_up > thumbs_down:
            current_now = db.get_user_points(guild_id, str(user.id))
            if current_now == 0:
                await interaction.channel.send(
                    f"Vote passed but {user.mention} already has **0** points."
                )
                return
            delta = -current_now
            zero_reason = f"x0: {reason}" if reason else "x0"
            db.add_transaction(
                guild_id=guild_id,
                from_user_id=str(from_user.id),
                to_user_id=str(user.id),
                points=delta,
                reason=zero_reason,
            )
            await interaction.channel.send(
                f"Vote passed ({thumbs_up} - {thumbs_down}). "
                f"{user.mention}'s points have been zeroed ({_fmt_points(current_now)} → **0**).",
                file=discord.File("waah-waa.gif"),
            )
        else:
            await interaction.channel.send(
                f"Vote failed ({thumbs_up} - {thumbs_down}). "
                f"{user.mention} keeps their **{_fmt_points(db.get_user_points(guild_id, str(user.id)))}** points."
            )

    asyncio.create_task(tally())


@tree.command(
    name="give",
    description="Give points to a user",
    guild=guild_obj,
)
@app_commands.describe(
    user="The user to give points to",
    points="Points to give (number, negative to deduct, or x2 to multiply)",
    reason="Optional reason for giving points",
)
async def give(
    interaction: discord.Interaction,
    user: discord.Member,
    points: str,
    reason: str | None = None,
):
    log.info(
        "/give invoked by %s — %s points to %s",
        interaction.user, points, user,
    )

    await interaction.response.defer()

    if user.id == interaction.user.id:
        await interaction.followup.send(
            "You can't give points to yourself.", ephemeral=True
        )
        return

    if user.bot:
        await interaction.followup.send(
            "You can't give points to a bot.", ephemeral=True
        )
        return

    guild_id = str(interaction.guild_id)
    stripped = points.strip()

    if stripped.lower().startswith("x"):
        # Multiplier mode: e.g. "x2", "x0.5", "x0"
        try:
            multiplier = Decimal(stripped[1:])
        except InvalidOperation:
            await interaction.followup.send(
                "Invalid multiplier. Use e.g. `x2` or `x0.5`.", ephemeral=True
            )
            return

        # x0 triggers a vote
        if multiplier == 0:
            await _handle_zero_vote(interaction, user, guild_id, reason)
            return

        current = db.get_user_points(guild_id, str(user.id))
        new_total = current * multiplier
        delta = new_total - current

        if delta == 0:
            await interaction.followup.send(
                f"{user.mention} already has **{_fmt_points(current)}** points. "
                f"Multiplying by {multiplier} changes nothing.",
                ephemeral=True,
            )
            return

        mult_reason = f"x{multiplier}: {reason}" if reason else f"x{multiplier}"

        db.add_transaction(
            guild_id=guild_id,
            from_user_id=str(interaction.user.id),
            to_user_id=str(user.id),
            points=delta,
            reason=mult_reason,
        )

        msg = f"Multiplied {user.mention}'s points by **{multiplier}** ({_fmt_points(current)} → **{_fmt_points(new_total)}**)"
        if reason:
            msg += f" — {reason}"
        await interaction.followup.send(msg)
        return

    # Regular number mode
    try:
        pts = Decimal(stripped)
    except InvalidOperation:
        await interaction.followup.send(
            "Invalid points value. Use a number or `x<multiplier>`.", ephemeral=True
        )
        return

    db.add_transaction(
        guild_id=guild_id,
        from_user_id=str(interaction.user.id),
        to_user_id=str(user.id),
        points=pts,
        reason=reason,
    )

    total = db.get_user_points(guild_id, str(user.id))

    if pts >= 0:
        msg = f"Gave **{_fmt_points(pts)}** point{'s' if pts != 1 else ''} to {user.mention}"
    else:
        msg = f"Removed **{_fmt_points(abs(pts))}** point{'s' if abs(pts) != 1 else ''} from {user.mention}"
    if reason:
        msg += f" — {reason}"
    msg += f"\n{user.mention} now has **{_fmt_points(total)}** point{'s' if total != 1 else ''} total."

    await interaction.followup.send(msg)


@tree.command(
    name="leaderboard",
    description="Show the points leaderboard",
    guild=guild_obj,
)
async def leaderboard(interaction: discord.Interaction):
    log.info("/leaderboard invoked by %s", interaction.user)

    await interaction.response.defer()

    guild_id = str(interaction.guild_id)
    rows = db.get_leaderboard(guild_id)

    if not rows:
        await interaction.followup.send(
            "No points have been given yet.", ephemeral=True
        )
        return

    medals = {1: "\U0001f947", 2: "\U0001f948", 3: "\U0001f949"}
    lines = []
    for rank, (user_id, total) in enumerate(rows, start=1):
        medal = medals.get(rank, "")
        prefix = f"{medal} " if medal else f"`{rank}.` "
        lines.append(
            f"{prefix}<@{user_id}> — **{_fmt_points(total)}** point{'s' if total != 1 else ''}"
        )

    embed = discord.Embed(
        title="Points Leaderboard",
        description="\n".join(lines),
        color=discord.Color.gold(),
    )

    await interaction.followup.send(embed=embed)


@tree.command(
    name="points",
    description="Check a user's points and recent history",
    guild=guild_obj,
)
@app_commands.describe(user="The user to check (defaults to yourself)")
async def points(
    interaction: discord.Interaction,
    user: discord.Member | None = None,
):
    target = user or interaction.user
    log.info("/points invoked by %s for %s", interaction.user, target)

    await interaction.response.defer()

    guild_id = str(interaction.guild_id)
    total = db.get_user_points(guild_id, str(target.id))
    history = db.get_user_history(guild_id, str(target.id))

    embed = discord.Embed(
        title=f"Points for {target.display_name}",
        color=discord.Color.blurple(),
    )
    embed.add_field(
        name="Total Points",
        value=_fmt_points(total),
        inline=False,
    )

    if history:
        lines = []
        for txn in history:
            ts = int(txn.created_at)
            if txn.reason and txn.reason.startswith("x"):
                line = f"**{txn.reason}** by <@{txn.from_user_id}> <t:{ts}:R>"
            else:
                sign = "+" if txn.points >= 0 else ""
                line = f"**{sign}{_fmt_points(txn.points)}** from <@{txn.from_user_id}> <t:{ts}:R>"
                if txn.reason:
                    line += f" — {txn.reason}"
            lines.append(line)
        embed.add_field(
            name="Recent History",
            value="\n".join(lines),
            inline=False,
        )

    await interaction.followup.send(embed=embed)


@tree.command(
    name="rules",
    description="Show a quick summary of all commands",
    guild=guild_obj,
)
async def rules(interaction: discord.Interaction):
    embed = discord.Embed(
        title="Point Counter — Commands",
        color=discord.Color.blue(),
        description=(
            "**/give <user> <points> [reason]**\n"
            "Give (or remove) points. Points can be a number, negative to deduct, "
            "or a multiplier like `x2` or `x0.5`.\n"
            "`x0` starts a 60-second vote to zero someone out (once per 24h).\n\n"
            "**/points [user]**\n"
            "Check your own (or another user's) total points and recent history.\n\n"
            "**/leaderboard**\n"
            "Show the top 10 users by total points.\n\n"
            "**/wipe**\n"
            "Delete all points data for this server (creates a backup first).\n\n"
            "**/rules**\n"
            "Show this message."
        ),
    )
    await interaction.response.send_message(embed=embed, ephemeral=True)


@tree.command(
    name="wipe",
    description="Delete all points data for this server",
    guild=guild_obj,
)
async def wipe(interaction: discord.Interaction):
    log.info("/wipe invoked by %s", interaction.user)

    await interaction.response.defer()

    guild_id = str(interaction.guild_id)
    count, backup = db.wipe_guild(guild_id)

    await interaction.followup.send(
        f"Wiped **{count}** transaction{'s' if count != 1 else ''} from the database.\n"
        f"Backup saved to `{backup}`."
    )


@client.event
async def on_ready():
    db.init_db()
    await tree.sync(guild=guild_obj)
    log.info("Bot ready — logged in as %s", client.user)


client.run(BOT_TOKEN)
