import ast as _ast
import asyncio
import cmath as _cmath
import json as _json
import logging
import math as _math
import os
import random as _random
import re as _re
import time
from decimal import Decimal, InvalidOperation
from fractions import Fraction

import anthropic
import discord
from discord import app_commands
from dotenv import load_dotenv

import db
import latex

load_dotenv()

BOT_TOKEN = os.environ["BOT_TOKEN"]
GUILD_ID = int(os.environ["GUILD_ID"])

VOTE_DURATION = 60  # seconds
ZERO_COOLDOWN = 86400  # 24 hours
NAN_COOLDOWN = 86400  # 24 hours

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("point-counter")

intents = discord.Intents.default()
client = discord.Client(intents=intents)
tree = app_commands.CommandTree(client)

guild_obj = discord.Object(id=GUILD_ID)
_anthropic = anthropic.AsyncAnthropic()


MAX_DISPLAY_LEN = 100  # Discord-safe length for a single number


def _is_nan(val: Decimal | Fraction | complex) -> bool:
    if isinstance(val, complex):
        return _math.isnan(val.real) or _math.isnan(val.imag)
    return isinstance(val, Decimal) and val.is_nan()


def _is_infinite(val: Decimal | Fraction | complex) -> bool:
    if isinstance(val, complex):
        return _math.isinf(val.real) or _math.isinf(val.imag)
    return isinstance(val, Decimal) and val.is_infinite()


def _fmt_float(f: float) -> str:
    """Format a float component of a complex number."""
    if _math.isnan(f):
        return "NaN"
    if _math.isinf(f):
        return "-Infinity" if f < 0 else "Infinity"
    if f == int(f):
        return str(int(f))
    return str(f)


def _fmt_points(val: Decimal | Fraction | complex) -> str:
    """Format a point value for display."""
    if isinstance(val, complex):
        r, im = val.real, val.imag
        has_real = r != 0 or _math.isnan(r)
        has_imag = im != 0 or _math.isnan(im)

        if not has_imag:
            return _fmt_float(r)

        # Format imaginary coefficient + "i"
        if _math.isnan(im):
            im_part = "NaN\u00b7i"
        elif abs(im) == 1:
            im_part = "i"
        else:
            im_part = f"{_fmt_float(abs(im))}i"

        if not has_real:
            return f"-{im_part}" if im < 0 else im_part

        # Both parts
        r_str = _fmt_float(r)
        if _math.isnan(im) or im > 0:
            return f"{r_str} + {im_part}"
        return f"{r_str} - {im_part}"

    if isinstance(val, Fraction):
        if val.denominator == 1:
            s = str(val.numerator)
            if len(s) > MAX_DISPLAY_LEN:
                return f"{Decimal(val.numerator).normalize():E} *(sci. notation)*"
            return s
        # If it's a terminating decimal, display as decimal
        d = val.denominator
        while d % 2 == 0:
            d //= 2
        while d % 5 == 0:
            d //= 5
        if d == 1:
            dec = Decimal(val.numerator) / Decimal(val.denominator)
            return _fmt_points(dec)
        frac_str = f"{val.numerator}/{val.denominator}"
        if len(frac_str) > MAX_DISPLAY_LEN:
            approx = Decimal(val.numerator) / Decimal(val.denominator)
            return f"≈{approx.normalize():E} *(fraction too long)*"
        return frac_str
    # Decimal path (also handles Infinity/NaN)
    if val.is_nan():
        return "NaN"
    if val.is_infinite():
        return "-Infinity" if val.is_signed() else "Infinity"
    # If the exponent is huge, skip int()/format('f') which would OOM expanding digits
    sign, digits, exponent = val.normalize().as_tuple()
    num_digits = len(digits) + max(exponent, 0) if isinstance(exponent, int) else 0
    if num_digits > MAX_DISPLAY_LEN:
        return f"{val.normalize():E} *(sci. notation — too long for Discord)*"
    if val == val.to_integral_value():
        return str(int(val))
    text = format(val.normalize(), 'f')
    if len(text) > MAX_DISPLAY_LEN:
        return f"{val.normalize():E} *(sci. notation — too long for Discord)*"
    return text


_EXPR_MAX_LEN = 200
_MAX_EXPONENT = 1000
_MAX_FRACTION_DENOM = 1000  # Fractions with larger denominators convert to Decimal

_EXPR_NAMES: dict[str, Decimal | complex] = {
    'e': Decimal('2.718281828459045235360287471352662497757247093699959574966'),
    'pi': Decimal('3.141592653589793238462643383279502884197169399375105820974'),
    'inf': Decimal('Infinity'),
    'i': complex(0, 1),
}

_EXPR_FUNCS = {
    'sqrt': lambda x: x.sqrt(),
    'abs': lambda x: abs(x),
    'log': lambda x: x.ln(),
    'log10': lambda x: x.log10(),
    'sin': lambda x: Decimal(str(_math.sin(float(x)))),
    'cos': lambda x: Decimal(str(_math.cos(float(x)))),
    'tan': lambda x: Decimal(str(_math.tan(float(x)))),
    'floor': lambda x: Decimal(_math.floor(x)),
    'ceil': lambda x: Decimal(_math.ceil(x)),
    'exp': lambda x: x.exp(),
    'round': lambda x: x.to_integral_value(),
    'sign': lambda x: Decimal('1') if x > 0 else (Decimal('-1') if x < 0 else Decimal('0')),
    'atan': lambda x: Decimal(str(_math.atan(float(x)))),
    'asin': lambda x: Decimal(str(_math.asin(float(x)))),
    'acos': lambda x: Decimal(str(_math.acos(float(x)))),
    'sinh': lambda x: Decimal(str(_math.sinh(float(x)))),
    'cosh': lambda x: Decimal(str(_math.cosh(float(x)))),
    'tanh': lambda x: Decimal(str(_math.tanh(float(x)))),
}

_COMPLEX_EXPR_FUNCS = {
    'sqrt': _cmath.sqrt,
    'log': _cmath.log,
    'log10': _cmath.log10,
    'sin': _cmath.sin,
    'cos': _cmath.cos,
    'tan': _cmath.tan,
    'exp': _cmath.exp,
    'atan': _cmath.atan,
    'asin': _cmath.asin,
    'acos': _cmath.acos,
    'sinh': _cmath.sinh,
    'cosh': _cmath.cosh,
    'tanh': _cmath.tanh,
}

_CALCULUS_FUNCS = {'diff', 'lim', 'integrate'}

_SYMPY_EVAL_FUNCS = {'factorial', 'gamma', 'fibonacci', 'nextprime', 'totient'}
_SYMPY_FUNC_LIMITS = {
    'factorial': 1000,
    'gamma': 1000,
    'fibonacci': 1000,
    'nextprime': 10**15,
    'totient': 10**15,
}


class _ExprError(Exception):
    pass


def _to_decimal(val: Decimal | Fraction) -> Decimal:
    """Convert Fraction to Decimal; pass Decimal through."""
    if isinstance(val, Fraction):
        return Decimal(val.numerator) / Decimal(val.denominator)
    return val


def _to_complex(val: Decimal | Fraction | complex) -> complex:
    """Convert any point value to complex."""
    if isinstance(val, complex):
        return val
    return complex(float(val), 0)


def _coerce(a, b) -> tuple:
    """Coerce a pair to compatible types for arithmetic."""
    if isinstance(a, complex) or isinstance(b, complex):
        return _to_complex(a), _to_complex(b)
    if type(a) is type(b):
        return a, b
    if isinstance(a, Fraction) and isinstance(b, Decimal):
        if b.is_nan() or b.is_infinite():
            return _to_decimal(a), b
        return a, Fraction(b)
    if isinstance(b, Fraction) and isinstance(a, Decimal):
        if a.is_nan() or a.is_infinite():
            return a, _to_decimal(b)
        return Fraction(a), b
    return a, b


def _from_sympy(result) -> Decimal | Fraction | complex:
    """Convert a sympy expression to Decimal, Fraction, or complex."""
    import sympy
    if result == sympy.oo:
        return Decimal('Infinity')
    if result == -sympy.oo:
        return Decimal('-Infinity')
    if result is sympy.nan or result == sympy.zoo:
        return Decimal('NaN')
    if hasattr(result, 'is_rational') and result.is_rational:
        r = sympy.Rational(result)
        num, den = int(r.p), int(r.q)
        if den == 1:
            return Decimal(num)
        d = den
        while d % 2 == 0:
            d //= 2
        while d % 5 == 0:
            d //= 5
        if d == 1:
            return Decimal(num) / Decimal(den)
        if den <= _MAX_FRACTION_DENOM:
            return Fraction(num, den)
        return Decimal(num) / Decimal(den)
    try:
        c = complex(result)
        if c.imag != 0:
            return c
        return Decimal(str(c.real))
    except (TypeError, ValueError, OverflowError):
        raise _ExprError(f"Could not convert result `{result}` to a number.")


def _eval_sympy_func(name: str, val: Decimal | Fraction | complex) -> Decimal | Fraction | complex:
    """Evaluate a sympy-backed function on an already-evaluated argument."""
    import sympy

    if isinstance(val, complex):
        raise _ExprError(f"`{name}` does not support complex numbers.")

    dec_val = _to_decimal(val)
    if dec_val.is_nan():
        return Decimal('NaN')

    limit = _SYMPY_FUNC_LIMITS.get(name)
    if limit and not dec_val.is_infinite() and abs(dec_val) > limit:
        raise _ExprError(f"`{name}` input too large (max ±{limit}).")

    # Convert to sympy
    if isinstance(val, Fraction):
        sv = sympy.Rational(val.numerator, val.denominator)
    elif dec_val.is_infinite():
        sv = sympy.oo if not dec_val.is_signed() else -sympy.oo
    else:
        sv = sympy.Rational(str(val))

    if name == 'nextprime':
        return Decimal(sympy.nextprime(int(sv)))

    if name in ('totient', 'fibonacci'):
        if not sv.is_integer:
            raise _ExprError(f"`{name}` requires an integer argument.")
        n = int(sv)
        if name == 'totient':
            if n < 1:
                raise _ExprError("`totient` requires a positive integer.")
            return Decimal(int(sympy.totient(n)))
        # fibonacci
        if n < 0:
            raise _ExprError("`fibonacci` requires a non-negative integer.")
        return Decimal(int(sympy.fibonacci(n)))

    if name == 'factorial':
        result = sympy.factorial(sv)
    elif name == 'gamma':
        result = sympy.gamma(sv)
    else:
        raise _ExprError(f"Unknown function `{name}`.")

    return _from_sympy(result)


def _eval_choose(a: Decimal | Fraction | complex, b: Decimal | Fraction | complex) -> Decimal | Fraction | complex:
    """Evaluate binomial coefficient choose(n, k)."""
    import sympy

    if isinstance(a, complex) or isinstance(b, complex):
        raise _ExprError("`choose` does not support complex numbers.")

    def _to_sym(val):
        if isinstance(val, Fraction):
            return sympy.Rational(val.numerator, val.denominator)
        d = _to_decimal(val)
        if d.is_nan():
            return sympy.nan
        if d.is_infinite():
            return sympy.oo if not d.is_signed() else -sympy.oo
        return sympy.Rational(str(val))

    if _to_decimal(a).is_nan() or _to_decimal(b).is_nan():
        return Decimal('NaN')

    return _from_sympy(sympy.binomial(_to_sym(a), _to_sym(b)))


def _safe_eval(expr_str: str, p: Decimal | Fraction | complex) -> Decimal | Fraction | complex:
    """Safely evaluate a math expression with variable p (current points)."""
    if len(expr_str) > _EXPR_MAX_LEN:
        raise _ExprError(f"Expression too long (max {_EXPR_MAX_LEN} chars).")
    expr_str = expr_str.replace('^', '**')
    try:
        tree = _ast.parse(expr_str, mode='eval')
    except SyntaxError as e:
        raise _ExprError(f"Invalid syntax: {e.msg}") from None
    result = _eval_node(tree.body, p)
    # Collapse pure-real complex back to Decimal
    if isinstance(result, complex) and result.imag == 0 and not _math.isnan(result.imag):
        return Decimal(str(result.real))
    return result


def _eval_node(node, p: Decimal | Fraction | complex) -> Decimal | Fraction | complex:
    if isinstance(node, _ast.Constant):
        if isinstance(node.value, (int, float)):
            return Decimal(str(node.value))
        raise _ExprError(f"Unsupported value: {node.value!r}")

    if isinstance(node, _ast.Name):
        if node.id == 'p':
            return p
        if node.id in _EXPR_NAMES:
            return _EXPR_NAMES[node.id]
        raise _ExprError(
            f"Unknown variable `{node.id}`. Use `p` for current points, `i` for imaginary unit."
        )

    if isinstance(node, _ast.UnaryOp):
        operand = _eval_node(node.operand, p)
        if isinstance(node.op, _ast.UAdd):
            return +operand
        if isinstance(node.op, _ast.USub):
            return -operand
        raise _ExprError("Unsupported unary operator.")

    if isinstance(node, _ast.BinOp):
        left = _eval_node(node.left, p)
        right = _eval_node(node.right, p)
        left, right = _coerce(left, right)

        if isinstance(node.op, _ast.Add):
            return left + right
        if isinstance(node.op, _ast.Sub):
            return left - right
        if isinstance(node.op, _ast.Mult):
            return left * right
        if isinstance(node.op, _ast.Div):
            return left / right
        if isinstance(node.op, _ast.Pow):
            if not _is_nan(right) and not _is_infinite(right) and abs(right) > _MAX_EXPONENT:
                raise _ExprError(f"Exponent too large (max ±{_MAX_EXPONENT}).")
            try:
                return left ** right
            except (InvalidOperation, ZeroDivisionError, OverflowError):
                # Fall back to complex (e.g. (-1)**0.5)
                return _to_complex(left) ** _to_complex(right)
        if isinstance(node.op, _ast.Mod):
            if isinstance(left, complex):
                raise _ExprError("Modulo is not supported for complex numbers.")
            return left % right
        raise _ExprError("Unsupported operator.")

    if isinstance(node, _ast.Call):
        if not isinstance(node.func, _ast.Name):
            raise _ExprError("Unsupported function call.")
        if node.keywords:
            raise _ExprError("Keyword arguments not supported.")

        name = node.func.id

        # Calculus functions — arguments stay symbolic
        if name in _CALCULUS_FUNCS:
            return _eval_calculus(name, node.args, p)

        # Regular functions — eagerly evaluate arguments
        args = [_eval_node(a, p) for a in node.args]

        if name in _EXPR_FUNCS and len(args) == 1:
            arg = args[0]
            if isinstance(arg, complex):
                if name == 'abs':
                    return Decimal(str(abs(arg)))
                if name in _COMPLEX_EXPR_FUNCS:
                    return _COMPLEX_EXPR_FUNCS[name](arg)
                raise _ExprError(f"`{name}` does not support complex numbers.")
            try:
                return _EXPR_FUNCS[name](_to_decimal(arg))
            except (InvalidOperation, ValueError):
                # Fall back to complex math (e.g. sqrt(-1), log(-1), asin(2))
                if name in _COMPLEX_EXPR_FUNCS:
                    return _COMPLEX_EXPR_FUNCS[name](complex(float(_to_decimal(arg))))
                raise
        if name in _SYMPY_EVAL_FUNCS and len(args) == 1:
            return _eval_sympy_func(name, args[0])
        if name == 'log' and len(args) == 2:
            if isinstance(args[0], complex) or isinstance(args[1], complex):
                return _cmath.log(_to_complex(args[0]), _to_complex(args[1]))
            return _to_decimal(args[0]).ln() / _to_decimal(args[1]).ln()
        if name == 'choose' and len(args) == 2:
            return _eval_choose(args[0], args[1])
        if name == 'mod' and len(args) == 2:
            a, b = _coerce(args[0], args[1])
            if isinstance(a, complex):
                raise _ExprError("`mod` does not support complex numbers.")
            return a % b
        all_funcs = (
            list(_EXPR_FUNCS) + list(_SYMPY_EVAL_FUNCS)
            + ['choose', 'mod'] + list(_CALCULUS_FUNCS)
        )
        funcs = ', '.join(f'`{n}`' for n in all_funcs)
        raise _ExprError(f"Unknown function `{name}`. Available: {funcs}.")

    raise _ExprError("Unsupported expression element.")


def _eval_calculus(name: str, arg_nodes: list, p: Decimal | Fraction | complex) -> Decimal | Fraction | complex:
    """Evaluate a calculus function using sympy. Returns Fraction for exact rational results."""
    import sympy
    sym_p = sympy.Symbol('p')

    def to_sym(node):
        if isinstance(node, _ast.Constant):
            if isinstance(node.value, (int, float)):
                return sympy.Rational(str(node.value))
            raise _ExprError(f"Unsupported value: {node.value!r}")
        if isinstance(node, _ast.Name):
            if node.id == 'p':
                return sym_p
            names = {'e': sympy.E, 'pi': sympy.pi, 'inf': sympy.oo, 'i': sympy.I}
            if node.id in names:
                return names[node.id]
            raise _ExprError(f"Unknown variable `{node.id}`.")
        if isinstance(node, _ast.UnaryOp):
            operand = to_sym(node.operand)
            if isinstance(node.op, _ast.UAdd):
                return operand
            if isinstance(node.op, _ast.USub):
                return -operand
        if isinstance(node, _ast.BinOp):
            left, right = to_sym(node.left), to_sym(node.right)
            ops = {
                _ast.Add: lambda l, r: l + r,
                _ast.Sub: lambda l, r: l - r,
                _ast.Mult: lambda l, r: l * r,
                _ast.Div: lambda l, r: l / r,
                _ast.Pow: lambda l, r: l ** r,
                _ast.Mod: lambda l, r: sympy.Mod(l, r),
            }
            for op_type, fn in ops.items():
                if isinstance(node.op, op_type):
                    return fn(left, right)
        if isinstance(node, _ast.Call):
            if not isinstance(node.func, _ast.Name):
                raise _ExprError("Unsupported function.")
            fn_name = node.func.id
            fn_args = [to_sym(a) for a in node.args]
            sym_funcs = {
                'sqrt': sympy.sqrt, 'abs': sympy.Abs,
                'log': sympy.log, 'log10': lambda x: sympy.log(x, 10),
                'sin': sympy.sin, 'cos': sympy.cos, 'tan': sympy.tan,
                'exp': sympy.exp,
                'floor': sympy.floor, 'ceil': sympy.ceiling,
                'diff': lambda x: sympy.diff(x, sym_p),
                'round': lambda x: sympy.floor(x + sympy.Rational(1, 2)),
                'sign': sympy.sign,
                'atan': sympy.atan, 'asin': sympy.asin, 'acos': sympy.acos,
                'sinh': sympy.sinh, 'cosh': sympy.cosh, 'tanh': sympy.tanh,
                'factorial': sympy.factorial,
                'gamma': sympy.gamma,
                'fibonacci': sympy.fibonacci,
            }
            if fn_name in sym_funcs:
                return sym_funcs[fn_name](*fn_args)
            if fn_name == 'choose' and len(fn_args) == 2:
                return sympy.binomial(fn_args[0], fn_args[1])
            if fn_name == 'mod' and len(fn_args) == 2:
                return sympy.Mod(fn_args[0], fn_args[1])
            raise _ExprError(f"Unknown function `{fn_name}` in calculus expression.")
        raise _ExprError("Unsupported expression in calculus function.")

    def to_result(sym_result) -> Decimal | Fraction | complex:
        # Substitute p if still symbolic
        if hasattr(sym_result, 'free_symbols') and sym_p in sym_result.free_symbols:
            sym_result = sym_result.subs(sym_p, val_to_sym(p))
        if sym_result == sympy.oo:
            return Decimal('Infinity')
        if sym_result == -sympy.oo:
            return Decimal('-Infinity')
        if sym_result is sympy.nan or sym_result == sympy.zoo:
            return Decimal('NaN')
        # Exact rational — return as Fraction only if it's a clean fraction
        if hasattr(sym_result, 'is_rational') and sym_result.is_rational:
            r = sympy.Rational(sym_result)
            num, den = int(r.p), int(r.q)
            if den == 1:
                return Decimal(num)
            # Terminating decimal (denominator is only 2s and 5s) — use Decimal
            d = den
            while d % 2 == 0:
                d //= 2
            while d % 5 == 0:
                d //= 5
            if d == 1:
                return Decimal(num) / Decimal(den)
            if den <= _MAX_FRACTION_DENOM:
                return Fraction(num, den)
            return Decimal(num) / Decimal(den)
        # Irrational or complex — fall back to float/complex
        try:
            result = complex(sym_result)
            if result.imag != 0:
                return result
            return Decimal(str(result.real))
        except (TypeError, ValueError, OverflowError):
            raise _ExprError(f"Could not convert result `{sym_result}` to a number.")

    def _float_to_sym(f):
        if _math.isnan(f):
            return sympy.nan
        if _math.isinf(f):
            return sympy.oo if f > 0 else -sympy.oo
        return sympy.Float(f)

    def val_to_sym(val):
        if isinstance(val, complex):
            return _float_to_sym(val.real) + sympy.I * _float_to_sym(val.imag)
        if isinstance(val, Fraction):
            return sympy.Rational(val.numerator, val.denominator)
        if val.is_nan():
            return sympy.nan
        if val.is_infinite():
            return sympy.oo if not val.is_signed() else -sympy.oo
        return sympy.Rational(str(val))

    if name == 'diff':
        if len(arg_nodes) != 1:
            raise _ExprError("`diff(expr)` takes 1 argument.")
        result = sympy.diff(to_sym(arg_nodes[0]), sym_p)
        return to_result(result)

    if name == 'lim':
        if len(arg_nodes) != 2:
            raise _ExprError("`lim(expr, value)` takes 2 arguments.")
        sym_expr = to_sym(arg_nodes[0])
        target = _eval_node(arg_nodes[1], p)
        result = sympy.limit(sym_expr, sym_p, val_to_sym(target))
        return to_result(result)

    if name == 'integrate':
        if len(arg_nodes) != 3:
            raise _ExprError("`integrate(expr, lower, upper)` takes 3 arguments.")
        sym_expr = to_sym(arg_nodes[0])
        lower = _eval_node(arg_nodes[1], p)
        upper = _eval_node(arg_nodes[2], p)
        result = sympy.integrate(sym_expr, (sym_p, val_to_sym(lower), val_to_sym(upper)))
        return to_result(result)

    raise _ExprError(f"Unknown calculus function `{name}`.")


def _parse_complex(s: str) -> complex | None:
    """Try to parse a string as a complex number (using 'i' notation). Returns None if not complex."""
    s = s.strip()
    if 'i' not in s:
        return None
    # Replace 'i' with 'j' for Python's complex parser
    js = s.replace('i', 'j').replace(' ', '')
    # Handle bare 'j' → '1j' (e.g. "j", "+j", "3+j")
    js = _re.sub(r'(?<![.\d])j', '1j', js)
    try:
        return complex(js)
    except (ValueError, TypeError):
        return None


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

    deadline = int(time.time()) + VOTE_DURATION

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
        await asyncio.sleep(VOTE_DURATION)
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
            # Infinity * 0 is indeterminate — zeroing produces NaN
            if _is_infinite(current_now):
                zero_reason = f"x0: {reason}" if reason else "x0"
                db.add_transaction(
                    guild_id=guild_id,
                    from_user_id=str(from_user.id),
                    to_user_id=str(user.id),
                    points=Decimal("NaN"),
                    reason=zero_reason,
                )
                await interaction.channel.send(
                    f"Vote passed ({thumbs_up} - {thumbs_down}). "
                    f"Infinity × 0 is indeterminate — {user.mention}'s points are now **NaN**.",
                    file=discord.File("waah-waa.gif"),
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


async def _handle_nan_vote(
    interaction: discord.Interaction,
    user: discord.Member,
    guild_id: str,
    reason: str | None,
):
    """Start a vote to NaN a user's points."""
    from_user = interaction.user

    # Check 24h cooldown
    last_nan = db.get_last_nan_give_time(guild_id, str(from_user.id))
    if last_nan is not None:
        elapsed = time.time() - last_nan
        if elapsed < NAN_COOLDOWN:
            next_available = int(last_nan + NAN_COOLDOWN)
            await interaction.followup.send(
                f"You can only NaN someone once every 24 hours. "
                f"Try again <t:{next_available}:R>.",
                ephemeral=True,
            )
            return

    current = db.get_user_points(guild_id, str(user.id))
    if _is_nan(current):
        await interaction.followup.send(
            f"{user.mention} already has **NaN** points.", ephemeral=True
        )
        return

    deadline = int(time.time()) + VOTE_DURATION

    embed = discord.Embed(
        title="NaN Vote",
        description=(
            f"Should {user.mention}'s points become **NaN**?\n"
            f"You have <t:{deadline}:R> to vote."
        ),
        color=discord.Color.dark_purple(),
    )
    if reason:
        embed.add_field(name="Reason", value=reason, inline=False)
    embed.set_footer(text=f"Called by {from_user.display_name}")

    vote_msg = await interaction.followup.send(embed=embed, wait=True)
    await vote_msg.add_reaction("\U0001f44d")
    await vote_msg.add_reaction("\U0001f44e")

    async def tally():
        await asyncio.sleep(VOTE_DURATION)
        try:
            vote_msg_updated = await interaction.channel.fetch_message(vote_msg.id)
        except discord.HTTPException:
            log.error("Could not fetch NaN vote message")
            return

        thumbs_up = 0
        thumbs_down = 0
        for reaction in vote_msg_updated.reactions:
            if str(reaction.emoji) == "\U0001f44d":
                thumbs_up = reaction.count - 1
            elif str(reaction.emoji) == "\U0001f44e":
                thumbs_down = reaction.count - 1

        if thumbs_up > thumbs_down:
            current_now = db.get_user_points(guild_id, str(user.id))
            if _is_nan(current_now):
                await interaction.channel.send(
                    f"Vote passed but {user.mention} already has **NaN** points."
                )
                return
            db.add_transaction(
                guild_id=guild_id,
                from_user_id=str(from_user.id),
                to_user_id=str(user.id),
                points=Decimal("NaN"),
                reason=reason,
            )
            await interaction.channel.send(
                f"Vote passed ({thumbs_up} - {thumbs_down}). "
                f"{user.mention}'s points are now **NaN**."
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
    points="Number, x<mult>, or =<expr> using p (e.g. =p^2, =sqrt(p))",
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

        try:
            if isinstance(current, complex):
                m = float(multiplier)
                new_total = complex(current.real * m, current.imag * m)
                delta = new_total - current
            else:
                cur_dec = _to_decimal(current)
                new_total = cur_dec * multiplier
                delta = new_total - cur_dec
        except (InvalidOperation, ValueError, OverflowError):
            # Indeterminate form (e.g. Infinity * 0, Infinity - Infinity)
            new_total = Decimal("NaN")
            delta = Decimal("NaN")

        # Inf * finite-positive = Inf, delta is indeterminate but result unchanged
        if new_total == current or (_is_nan(new_total) and _is_nan(current)):
            await interaction.followup.send(
                f"{user.mention} already has **{_fmt_points(current)}** points. "
                f"Multiplying by {multiplier} changes nothing.",
                ephemeral=True,
            )
            return

        # If the result is NaN, route through the NaN vote
        if _is_nan(new_total):
            await _handle_nan_vote(interaction, user, guild_id, reason)
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

        latex_str = latex.to_latex(old=current, new=new_total, reason=mult_reason)
        latex_file = latex.render_latex(latex_str) if latex_str else None
        if latex_file:
            embed = discord.Embed(description=msg, color=discord.Color.blurple())
            embed.set_image(url="attachment://math.png")
            await interaction.followup.send(embed=embed, file=latex_file)
        else:
            await interaction.followup.send(msg)
        return

    # Expression mode: e.g. "=p^2", "=sqrt(p)+1"
    if stripped.startswith("="):
        expr = stripped[1:].strip()
        if not expr:
            await interaction.followup.send("Empty expression.", ephemeral=True)
            return

        current = db.get_user_points(guild_id, str(user.id))

        if _is_nan(current):
            await interaction.followup.send(
                f"{user.mention} has **NaN** points — use `/unnan` first.",
                ephemeral=True,
            )
            return

        try:
            new_total = _safe_eval(expr, current)
        except _ExprError as e:
            await interaction.followup.send(str(e), ephemeral=True)
            return
        except Exception:
            await interaction.followup.send(
                "Expression produced a math error.", ephemeral=True
            )
            return

        # If result is NaN, route through vote
        if _is_nan(new_total):
            await _handle_nan_vote(interaction, user, guild_id, reason)
            return

        # If nothing changed
        if new_total == current:
            await interaction.followup.send(
                f"Expression changes nothing — {user.mention} stays at "
                f"**{_fmt_points(current)}** points.",
                ephemeral=True,
            )
            return

        expr_reason = f"={expr}: {reason}" if reason else f"={expr}"

        if _is_infinite(current):
            # Can't compute a delta from infinity; store as a reset
            db.add_transaction(
                guild_id=guild_id,
                from_user_id=str(interaction.user.id),
                to_user_id=str(user.id),
                points=new_total,
                reason=expr_reason,
                is_reset=True,
            )
        else:
            nt, cur = _coerce(new_total, current)
            delta = nt - cur
            db.add_transaction(
                guild_id=guild_id,
                from_user_id=str(interaction.user.id),
                to_user_id=str(user.id),
                points=delta,
                reason=expr_reason,
            )

        msg = f"Applied `={expr}` to {user.mention}'s points ({_fmt_points(current)} → **{_fmt_points(new_total)}**)"
        if reason:
            msg += f" — {reason}"

        latex_str = latex.to_latex(old=current, new=new_total, reason=expr_reason)
        latex_file = latex.render_latex(latex_str) if latex_str else None
        if latex_file:
            embed = discord.Embed(description=msg, color=discord.Color.blurple())
            embed.set_image(url="attachment://math.png")
            await interaction.followup.send(embed=embed, file=latex_file)
        else:
            await interaction.followup.send(msg)
        return

    # Regular number mode — try complex first, then Decimal
    pts = _parse_complex(stripped)
    if pts is None:
        try:
            pts = Decimal(stripped)
        except InvalidOperation:
            await interaction.followup.send(
                "Invalid points value. Use a number, complex (`3+4i`), `x<multiplier>`, or `=<expression>`.",
                ephemeral=True,
            )
            return

    # NaN triggers a vote
    if _is_nan(pts):
        await _handle_nan_vote(interaction, user, guild_id, reason)
        return

    # Check if adding infinite points would produce an indeterminate result
    if _is_infinite(pts):
        current = db.get_user_points(guild_id, str(user.id))
        try:
            if isinstance(pts, complex) or isinstance(current, complex):
                _ = _to_complex(current) + _to_complex(pts)
            else:
                _ = _to_decimal(current) + pts
        except (InvalidOperation, ValueError, OverflowError):
            # e.g. Infinity + (-Infinity) — indeterminate, route to NaN vote
            await _handle_nan_vote(interaction, user, guild_id, reason)
            return

    db.add_transaction(
        guild_id=guild_id,
        from_user_id=str(interaction.user.id),
        to_user_id=str(user.id),
        points=pts,
        reason=reason,
    )

    total = db.get_user_points(guild_id, str(user.id))

    if isinstance(pts, complex):
        msg = f"Gave **{_fmt_points(pts)}** to {user.mention}"
    elif pts >= 0:
        msg = f"Gave **{_fmt_points(pts)}** point{'s' if pts != 1 else ''} to {user.mention}"
    else:
        msg = f"Removed **{_fmt_points(abs(pts))}** point{'s' if abs(pts) != 1 else ''} from {user.mention}"
    if reason:
        msg += f" — {reason}"
    msg += f"\n{user.mention} now has **{_fmt_points(total)}** point{'s' if total != 1 else ''} total."

    if latex.needs_latex(total):
        latex_str = latex.to_latex(new=total)
        latex_file = latex.render_latex(latex_str) if latex_str else None
        if latex_file:
            embed = discord.Embed(description=msg, color=discord.Color.blurple())
            embed.set_image(url="attachment://math.png")
            await interaction.followup.send(embed=embed, file=latex_file)
            return

    await interaction.followup.send(msg)


@tree.command(
    name="judge",
    description="Ask Claude to judge how many points someone deserves",
    guild=guild_obj,
)
@app_commands.describe(
    user="The user to judge",
    statement="What they did (Claude decides the points)",
)
async def judge(
    interaction: discord.Interaction,
    user: discord.Member,
    statement: str,
):
    log.info("/judge invoked by %s — judging %s: %s", interaction.user, user, statement)

    await interaction.response.defer()

    if user.bot:
        await interaction.followup.send(
            "You can't give points to a bot.", ephemeral=True
        )
        return

    guild_id = str(interaction.guild_id)
    current = db.get_user_points(guild_id, str(user.id))

    try:
        msg = await _anthropic.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=256,
            system=(
                "You are the Judge — a deeply opinionated, unhinged arbiter of points in a Discord server. "
                "You take your job extremely seriously despite the fact that points mean nothing. "
                "A user describes what someone did and you decide how many points they deserve.\n"
                "Respond with ONLY a JSON object, no markdown: "
                '{"points": "<value>", "reason": "<one-sentence explanation>"}\n'
                "The points value can be any of:\n"
                "- A plain number: 10, -50, 3.5\n"
                "- A complex number: 3+4i, i, -2i\n"
                "- A multiplier on current points: x2, x0.5, x-1, x0 (zeroes them out)\n"
                "- A math expression using p (current points): =p^2, =sqrt(p), =p+1, =log(p)\n"
                "  Functions: sqrt, abs, log, exp, sin, cos, tan, floor, ceil, factorial, fibonacci, diff, integrate\n"
                "- Special values: Infinity, -Infinity, NaN (the nuclear option — makes their points permanently undefined)\n"
                f"The target currently has {_fmt_points(current)} points.\n"
                "Your reason should be one short punchy sentence. Not a corporate performance review. "
                "Talk like a real person — be blunt, sarcastic, unhinged, or genuinely impressed as appropriate. "
                "Have actual opinions. Pick sides. Be petty sometimes. "
                "For plain numbers: small mundane things 1-10, solid 20-80, exceptional 100-300, legendary 300+. "
                "Negative for bad behavior. Use multipliers, expressions, or special values when the vibes call for it. "
                "NEVER give 0 plain points (x0 is fine though). Always commit to a verdict."
            ),
            messages=[{"role": "user", "content": statement}],
        )
    except anthropic.APIError as e:
        log.error("Anthropic API error: %s", e)
        await interaction.followup.send(
            "The judge is unavailable right now. Try again later.", ephemeral=True
        )
        return

    raw = msg.content[0].text.strip()
    if raw.startswith("```"):
        raw = _re.sub(r"^```\w*\n?|```$", "", raw).strip()
    try:
        data = _json.loads(raw)
        points_str = str(data["points"]).strip()
        reason = str(data["reason"])
    except (ValueError, KeyError, TypeError, _json.JSONDecodeError):
        log.error("Failed to parse judge response: %s", raw)
        await interaction.followup.send(
            "The judge mumbled something incoherent. Try again.", ephemeral=True
        )
        return

    # --- Parse and apply the judge's verdict (same formats as /give, no votes) ---
    from_id = str(interaction.user.id)
    to_id = str(user.id)

    try:
        if points_str.lower().startswith("x"):
            # Multiplier mode
            multiplier = Decimal(points_str[1:])
            try:
                if isinstance(current, complex):
                    m = float(multiplier)
                    new_total = complex(current.real * m, current.imag * m)
                    delta = new_total - current
                else:
                    cur_dec = _to_decimal(current)
                    new_total = cur_dec * multiplier
                    delta = new_total - cur_dec
            except (InvalidOperation, ValueError, OverflowError):
                new_total = Decimal("NaN")
                delta = Decimal("NaN")

            mult_reason = f"x{multiplier}: judge — {reason}"
            db.add_transaction(
                guild_id=guild_id, from_user_id=from_id, to_user_id=to_id,
                points=delta, reason=mult_reason,
            )
            verdict = f"Multiplied {user.mention}'s points by **{multiplier}** ({_fmt_points(current)} → **{_fmt_points(new_total)}**)"

        elif points_str.startswith("="):
            # Expression mode
            expr = points_str[1:].strip()
            if not expr:
                raise _ExprError("Empty expression.")

            new_total = _safe_eval(expr, current)
            expr_reason = f"={expr}: judge — {reason}"

            if _is_infinite(current):
                db.add_transaction(
                    guild_id=guild_id, from_user_id=from_id, to_user_id=to_id,
                    points=new_total, reason=expr_reason, is_reset=True,
                )
            else:
                nt, cur = _coerce(new_total, current)
                delta = nt - cur
                db.add_transaction(
                    guild_id=guild_id, from_user_id=from_id, to_user_id=to_id,
                    points=delta, reason=expr_reason,
                )
            verdict = f"Applied `={expr}` to {user.mention}'s points ({_fmt_points(current)} → **{_fmt_points(new_total)}**)"

        else:
            # Plain number — try complex, then Decimal
            pts = _parse_complex(points_str)
            if pts is None:
                pts = Decimal(points_str)

            db.add_transaction(
                guild_id=guild_id, from_user_id=from_id, to_user_id=to_id,
                points=pts, reason=f"judge: {reason}",
            )

            if isinstance(pts, complex) or _is_nan(pts):
                verdict = f"Gave **{_fmt_points(pts)}** to {user.mention}"
            elif pts >= 0:
                verdict = f"Gave **{_fmt_points(pts)}** point{'s' if pts != 1 else ''} to {user.mention}"
            else:
                verdict = f"Removed **{_fmt_points(abs(pts))}** point{'s' if abs(pts) != 1 else ''} from {user.mention}"

    except (_ExprError, InvalidOperation) as e:
        log.error("Judge points parse error for '%s': %s", points_str, e)
        await interaction.followup.send(
            "The judge rendered an impossible verdict. Try again.", ephemeral=True
        )
        return

    total = db.get_user_points(guild_id, str(user.id))

    embed = discord.Embed(
        description=(
            f"**Statement:** {statement}\n\n"
            f"**Verdict:** {verdict}\n"
            f"**Reason:** {reason}\n\n"
            f"{user.mention} now has **{_fmt_points(total)}** point{'s' if total != 1 else ''} total."
        ),
        color=discord.Color.orange(),
    )
    embed.set_author(name="The Judge")
    embed.set_footer(text=f"Requested by {interaction.user.display_name}")

    await interaction.followup.send(embed=embed)


# ---------------------------------------------------------------------------
# Leaderboard sort helpers
# ---------------------------------------------------------------------------

_NAMED_SORTS = {'total', 'bottom', 'magnitude', 'real', 'imag', 'random'}


def _magnitude_sort_key(item):
    val = item[1]
    if _is_nan(val):
        return (-1, Fraction(0))
    if _is_infinite(val):
        return (1, Fraction(0))
    if isinstance(val, complex):
        return (0, Fraction(abs(val)))
    return (0, Fraction(abs(_to_decimal(val))))


def _real_sort_key(item):
    val = item[1]
    if isinstance(val, complex):
        r = val.real
        if _math.isnan(r):
            return (-3, Fraction(0))
        if _math.isinf(r):
            return (2 if r > 0 else -2, Fraction(0))
        return (1, Fraction(r))
    return db.default_sort_key(item)


def _imag_sort_key(item):
    val = item[1]
    if isinstance(val, complex):
        im = val.imag
        if _math.isnan(im):
            return (-3, Fraction(0))
        if _math.isinf(im):
            return (2 if im > 0 else -2, Fraction(0))
        return (1, Fraction(im))
    return (1, Fraction(0))


def _sort_leaderboard(
    totals: dict[str, db.Points],
    sort_mode: str,
    limit: int = 10,
) -> tuple[list[tuple[str, db.Points, db.Points | None]], str]:
    """Sort totals by the given mode.

    Returns (sorted_list, title_suffix).
    Each item: (user_id, original_total, sort_value_or_None).
    sort_value is non-None only for expression sorts.
    """
    items = list(totals.items())

    # Expression mode
    if sort_mode.startswith('='):
        expr = sort_mode[1:]
        if not expr.strip():
            raise _ExprError("Empty expression.")
        results = []
        for user_id, total in items:
            try:
                sort_val = _safe_eval(expr, total)
            except Exception:
                sort_val = None
            results.append((user_id, total, sort_val))

        def _expr_sort_key(item):
            sv = item[2]
            if sv is None:
                return (-4, Fraction(0))
            return db.default_sort_key(('', sv))

        results.sort(key=_expr_sort_key, reverse=True)
        display_expr = expr if len(expr) <= 50 else expr[:50] + '...'
        return results[:limit], f"by ={display_expr}"

    # Named modes
    mode = sort_mode.lower()
    if mode not in _NAMED_SORTS:
        raise _ExprError(
            f"Unknown sort mode `{sort_mode}`. "
            f"Available: {', '.join(sorted(_NAMED_SORTS))}, or `=<expr>`."
        )

    if mode == 'total':
        items.sort(key=db.default_sort_key, reverse=True)
        suffix = ""
    elif mode == 'bottom':
        items.sort(key=db.default_sort_key, reverse=False)
        suffix = "bottom"
    elif mode == 'magnitude':
        items.sort(key=_magnitude_sort_key, reverse=True)
        suffix = "by magnitude"
    elif mode == 'real':
        items.sort(key=_real_sort_key, reverse=True)
        suffix = "by real part"
    elif mode == 'imag':
        items.sort(key=_imag_sort_key, reverse=True)
        suffix = "by imaginary part"
    elif mode == 'random':
        _random.shuffle(items)
        suffix = "shuffled"

    return [(uid, tot, None) for uid, tot in items[:limit]], suffix


@tree.command(
    name="leaderboard",
    description="Show the points leaderboard",
    guild=guild_obj,
)
@app_commands.describe(
    sort="Sort mode: total, bottom, magnitude, real, imag, random, or =<expr> (e.g. =abs(p), =p^2)"
)
async def leaderboard(interaction: discord.Interaction, sort: str | None = None):
    log.info("/leaderboard invoked by %s (sort=%s)", interaction.user, sort)

    await interaction.response.defer()

    guild_id = str(interaction.guild_id)
    totals = db.get_guild_totals(guild_id)

    if not totals:
        await interaction.followup.send(
            "No points have been given yet.", ephemeral=True
        )
        return

    sort_mode = sort or 'total'
    try:
        rows, title_suffix = _sort_leaderboard(totals, sort_mode)
    except _ExprError as e:
        await interaction.followup.send(str(e), ephemeral=True)
        return

    medals = {1: "\U0001f947", 2: "\U0001f948", 3: "\U0001f949"}
    lines = []
    is_expr = sort_mode.startswith('=')
    for rank, (user_id, total, sort_val) in enumerate(rows, start=1):
        medal = medals.get(rank, "")
        prefix = f"{medal} " if medal else f"`{rank}.` "
        entry = f"{prefix}<@{user_id}> — **{_fmt_points(total)}** point{'s' if total != 1 else ''}"
        if is_expr:
            sv_str = _fmt_points(sort_val) if sort_val is not None else "error"
            entry += f" ({sort_mode} → {sv_str})"
        lines.append(entry)

    title = "Points Leaderboard"
    if title_suffix:
        title += f" ({title_suffix})"

    embed = discord.Embed(
        title=title,
        description="\n".join(lines),
        color=discord.Color.gold(),
    )

    await interaction.followup.send(embed=embed)


@leaderboard.autocomplete('sort')
async def _leaderboard_sort_autocomplete(
    interaction: discord.Interaction, current: str
) -> list[app_commands.Choice[str]]:
    if current.startswith('='):
        return [app_commands.Choice(name=current, value=current)]
    choices = [
        app_commands.Choice(name=m, value=m)
        for m in sorted(_NAMED_SORTS)
        if current.lower() in m
    ]
    return choices[:25]


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

    latex_file = None
    if latex.needs_latex(total):
        latex_str = latex.to_latex(new=total)
        if latex_str:
            latex_file = latex.render_latex(latex_str)
            if latex_file:
                embed.set_thumbnail(url="attachment://math.png")

    if history:
        lines = []
        for txn in history:
            ts = int(txn.created_at)
            if txn.reason and (txn.reason.startswith("x") or txn.reason.startswith("=")):
                line = f"**{txn.reason}** by <@{txn.from_user_id}> <t:{ts}:R>"
            else:
                sign = "+" if isinstance(txn.points, complex) or _is_nan(txn.points) or txn.points >= 0 else ""
                line = f"**{sign}{_fmt_points(txn.points)}** from <@{txn.from_user_id}> <t:{ts}:R>"
                if txn.reason:
                    line += f" — {txn.reason}"
            lines.append(line)
        embed.add_field(
            name="Recent History",
            value="\n".join(lines),
            inline=False,
        )

    if latex_file:
        await interaction.followup.send(embed=embed, file=latex_file)
    else:
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
            "Give (or remove) points. Points can be a number, complex (`3+4i`, `i`), negative to deduct, "
            "a multiplier like `x2` or `x0.5`, "
            "or an expression like `=p^2` or `=sqrt(p)` (where `p` = current points, `i` = imaginary unit).\n"
            "**Math:** `sqrt`, `abs`, `exp`, `log`, `log10`, `round`, `sign`, `mod`\n"
            "**Trig:** `sin`, `cos`, `tan`, `asin`, `acos`, `atan`, `sinh`, `cosh`, `tanh`\n"
            "**Rounding:** `floor`, `ceil`, `round`\n"
            "**Combinatorics:** `factorial`, `gamma`, `fibonacci`, `choose(n, k)`\n"
            "**Number theory:** `nextprime`, `totient`\n"
            "**Calculus:** `diff(p^3)`, `lim(sin(p)/p, 0)`, `integrate(p^2, 0, p)`\n"
            "**Complex:** `i` constant, `sqrt(-1)` → `i`, complex arithmetic. "
            "Functions like `sqrt`, `log`, trig auto-extend to complex domain.\n"
            "Rational results are stored as exact fractions. "
            "⚠️ Trig/irrational/complex results use float precision.\n"
            "`x0` starts a 60-second vote to zero someone out (once per 24h).\n"
            "`NaN` starts a 60-second vote to curse someone with NaN points (once per 24h).\n\n"
            "**/points [user]**\n"
            "Check your own (or another user's) total points and recent history.\n\n"
            "**/leaderboard [sort]**\n"
            "Show the top 10. Sort: `total` (default), `bottom`, `magnitude`, `real`, `imag`, `random`, "
            "or `=<expr>` (e.g. `=abs(p)`).\n\n"
            "**/judge <user> <statement>**\n"
            "Ask the Judge (Claude AI) to decide how many points someone deserves. "
            "The Judge can use any point format — numbers, complex, multipliers (`x2`), "
            "expressions (`=p^2`), even NaN. The Judge's word is law (no votes).\n\n"
            "**/selfnan**\n"
            "Curse yourself with NaN points. No vote needed.\n\n"
            "**/unnan**\n"
            "Start a vote to remove NaN from your own points. "
            "If the vote passes, all NaN transactions are deleted and your points are restored.\n\n"
            "**/wipe**\n"
            "Delete all points data for this server (creates a backup first).\n\n"
            "**/explain <topic>**\n"
            "Explains how a specific expression, function, or format works. Use autocomplete to browse topics.\n\n"
            "**/rules**\n"
            "Show this message."
        ),
    )
    await interaction.response.send_message(embed=embed, ephemeral=True)


# ---------------------------------------------------------------------------
# /explain — per-expression help
# ---------------------------------------------------------------------------

_EXPLAIN_ENTRIES: dict[str, tuple[str, str]] = {
    # Formats
    "number": (
        "Plain number — positive, negative, or decimal.",
        "`/give @user 10` → gives 10 points\n`/give @user -3.5` → removes 3.5 points",
    ),
    "complex": (
        "Complex number using `i` (the imaginary unit). Both parts are tracked separately.",
        "`/give @user 3+4i` → gives 3 + 4i points\n`/give @user i` → gives i\n`/give @user -2i` → gives −2i",
    ),
    "multiplier": (
        "Multiply someone's current points by a value. Written as `x` followed by a number.\n"
        "`x0` triggers a vote to zero someone out (24h cooldown).",
        "`/give @user x2` → doubles their points\n`/give @user x0.5` → halves them\n`/give @user x-1` → negates them\n`/give @user x0` → starts a vote to zero them",
    ),
    "expression": (
        "A math expression using `p` for the target's current points. Written as `=` followed by the expression.\n"
        "All functions listed below are available inside expressions.",
        "`/give @user =p^2` → squares their points\n`/give @user =p+10` → adds 10\n`/give @user =sqrt(p)` → square root",
    ),
    # Math functions
    "sqrt": (
        "Square root. Automatically extends to complex domain for negative inputs (returns `i`).",
        "`=sqrt(p)` — if p = 16, result is 4\n`=sqrt(-1)` → i",
    ),
    "abs": (
        "Absolute value. Works on real, negative, and complex numbers.",
        "`=abs(p)` — if p = -5, result is 5\n`=abs(3+4i)` → 5",
    ),
    "log": (
        "Natural logarithm (base e). Two-arg form `log(x, base)` uses a custom base.\n"
        "Falls back to complex domain for negative inputs.",
        "`=log(p)` — if p = e, result is 1\n`=log(p, 10)` — log base 10\n`=log(-1)` → πi",
    ),
    "log10": (
        "Log base 10.",
        "`=log10(p)` — if p = 100, result is 2",
    ),
    "exp": (
        "Exponential function (e^x).",
        "`=exp(p)` — if p = 1, result is e ≈ 2.718",
    ),
    "round": (
        "Round to the nearest integer.",
        "`=round(p)` — if p = 3.7, result is 4",
    ),
    "sign": (
        "Returns −1, 0, or 1 based on the sign of the value.",
        "`=sign(p)` — if p = -42, result is -1",
    ),
    "mod": (
        "Modulo (remainder). Takes two arguments.",
        "`=mod(p, 3)` — if p = 10, result is 1",
    ),
    # Trig
    "sin": (
        "Sine (radians). Extends to complex domain.",
        "`=sin(p)` — if p = 0, result is 0\n`=sin(pi/2)` → 1",
    ),
    "cos": (
        "Cosine (radians). Extends to complex domain.",
        "`=cos(p)` — if p = 0, result is 1\n`=cos(pi)` → -1",
    ),
    "tan": (
        "Tangent (radians). Extends to complex domain.",
        "`=tan(p)` — if p = 0, result is 0",
    ),
    "asin": (
        "Inverse sine. Returns radians. Complex domain for |x| > 1.",
        "`=asin(p)` — if p = 1, result is π/2",
    ),
    "acos": (
        "Inverse cosine. Returns radians. Complex domain for |x| > 1.",
        "`=acos(p)` — if p = 1, result is 0",
    ),
    "atan": (
        "Inverse tangent. Returns radians.",
        "`=atan(p)` — if p = 1, result is π/4",
    ),
    "sinh": (
        "Hyperbolic sine.",
        "`=sinh(p)` — if p = 0, result is 0",
    ),
    "cosh": (
        "Hyperbolic cosine.",
        "`=cosh(p)` — if p = 0, result is 1",
    ),
    "tanh": (
        "Hyperbolic tangent.",
        "`=tanh(p)` — if p = 0, result is 0",
    ),
    # Rounding
    "floor": (
        "Round down to the nearest integer.",
        "`=floor(p)` — if p = 3.7, result is 3\n`=floor(-2.3)` → -3",
    ),
    "ceil": (
        "Round up to the nearest integer.",
        "`=ceil(p)` — if p = 3.2, result is 4\n`=ceil(-2.7)` → -2",
    ),
    # Combinatorics / number theory
    "factorial": (
        "Factorial (n!). Max input: 1000. No complex numbers.",
        "`=factorial(p)` — if p = 5, result is 120",
    ),
    "gamma": (
        "Gamma function — the continuous generalization of factorial: Γ(n) = (n−1)!. Max input: 1000.",
        "`=gamma(p)` — if p = 6, result is 120 (same as 5!)",
    ),
    "fibonacci": (
        "Nth Fibonacci number. Max input: 1000.",
        "`=fibonacci(p)` — if p = 10, result is 55",
    ),
    "choose": (
        "Binomial coefficient C(n, k). Two arguments.",
        "`=choose(p, 2)` — if p = 10, result is 45\n`=choose(52, 5)` → 2598960",
    ),
    "nextprime": (
        "Smallest prime greater than the input. Max input: 10^15.",
        "`=nextprime(p)` — if p = 10, result is 11",
    ),
    "totient": (
        "Euler's totient φ(n) — count of integers ≤ n that are coprime to n. Max input: 10^15.",
        "`=totient(p)` — if p = 12, result is 4",
    ),
    # Calculus
    "diff": (
        "Symbolic differentiation with respect to p, evaluated at the user's current points.\n"
        "Differentiates the expression, then substitutes p.",
        "`=diff(p^3)` — derivative is 3p², if p = 2, result is 12\n`=diff(sin(p))` → cos(p)",
    ),
    "integrate": (
        "Definite integral. Takes the expression, lower bound, and upper bound.\n"
        "Bounds can use `p` for current points.",
        "`=integrate(p^2, 0, p)` — integral of x² from 0 to p, result is p³/3\n`=integrate(p, 0, 10)` → 50",
    ),
    "lim": (
        "Limit of an expression as p approaches a value.",
        "`=lim(sin(p)/p, 0)` → 1\n`=lim(1/p, 0)` → Infinity",
    ),
    # Special values
    "Infinity": (
        "Positive infinity. Contagious in arithmetic — most operations with Infinity return Infinity.\n"
        "Infinity + (-Infinity) or Infinity × 0 produces NaN (triggers a vote via `/give`).",
        "`/give @user Infinity` → their points become Infinity\n`=log(0)` → -Infinity",
    ),
    "NaN": (
        "Not a Number. The nuclear option — **permanently** corrupts a user's points.\n"
        "NaN is contagious: any arithmetic with NaN produces NaN.\n"
        "Only removable via `/unnan` (vote) or `/wipe`. Giving NaN via `/give` triggers a vote (24h cooldown).",
        "`/give @user NaN` → starts a NaN vote\n`/selfnan` → curse yourself directly",
    ),
    "i": (
        "The imaginary unit (√−1). Available as a constant in expressions and as part of complex number literals.",
        "`/give @user i` → gives i points\n`=p * i` → rotates points 90° in the complex plane\n`=sqrt(-1)` → i",
    ),
    # Constants
    "pi": (
        "π ≈ 3.14159. Available as a constant in expressions.",
        "`=pi` → sets points to π\n`=p * pi` → multiplies points by π",
    ),
    "e": (
        "Euler's number e ≈ 2.71828. Available as a constant in expressions.",
        "`=e` → sets points to e\n`=e^p` → exponential",
    ),
}

_EXPLAIN_NAMES = sorted(_EXPLAIN_ENTRIES.keys())


@tree.command(
    name="explain",
    description="Explain how a point expression or function works",
    guild=guild_obj,
)
@app_commands.describe(topic="The expression, function, or format to explain")
async def explain(interaction: discord.Interaction, topic: str):
    key = topic.strip().lower()
    entry = _EXPLAIN_ENTRIES.get(key)
    if entry is None:
        await interaction.response.send_message(
            f"Unknown topic `{topic}`. Use autocomplete to see available topics.",
            ephemeral=True,
        )
        return

    description, examples = entry
    embed = discord.Embed(
        title=topic,
        color=discord.Color.blue(),
    )
    embed.add_field(name="Description", value=description, inline=False)
    embed.add_field(name="Examples", value=examples, inline=False)
    await interaction.response.send_message(embed=embed, ephemeral=True)


@explain.autocomplete('topic')
async def _explain_autocomplete(
    interaction: discord.Interaction, current: str,
) -> list[app_commands.Choice[str]]:
    matches = [
        app_commands.Choice(name=name, value=name)
        for name in _EXPLAIN_NAMES
        if current.lower() in name.lower()
    ]
    return matches[:25]


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


@tree.command(
    name="selfnan",
    description="Curse yourself with NaN points",
    guild=guild_obj,
)
async def selfnan(interaction: discord.Interaction):
    log.info("/selfnan invoked by %s", interaction.user)

    await interaction.response.defer()

    guild_id = str(interaction.guild_id)
    user_id = str(interaction.user.id)

    current = db.get_user_points(guild_id, user_id)
    if _is_nan(current):
        await interaction.followup.send(
            "You already have **NaN** points.", ephemeral=True
        )
        return

    db.add_transaction(
        guild_id=guild_id,
        from_user_id=user_id,
        to_user_id=user_id,
        points=Decimal("NaN"),
        reason="selfnan",
    )

    await interaction.followup.send(
        f"{interaction.user.mention} has cursed themselves with **NaN** points."
    )


@tree.command(
    name="unnan",
    description="Start a vote to remove NaN from your points",
    guild=guild_obj,
)
async def unnan(interaction: discord.Interaction):
    log.info("/unnan invoked by %s", interaction.user)

    await interaction.response.defer()

    guild_id = str(interaction.guild_id)
    user_id = str(interaction.user.id)

    if not db.is_user_nan(guild_id, user_id):
        await interaction.followup.send(
            "Your points aren't NaN.", ephemeral=True
        )
        return

    deadline = int(time.time()) + VOTE_DURATION
    user = interaction.user

    embed = discord.Embed(
        title="UnNaN Vote",
        description=(
            f"Should {user.mention} be freed from **NaN**?\n"
            f"You have <t:{deadline}:R> to vote."
        ),
        color=discord.Color.green(),
    )
    embed.set_footer(text=f"Requested by {user.display_name}")

    vote_msg = await interaction.followup.send(embed=embed, wait=True)
    await vote_msg.add_reaction("\U0001f44d")
    await vote_msg.add_reaction("\U0001f44e")

    async def tally():
        await asyncio.sleep(VOTE_DURATION)
        try:
            vote_msg_updated = await interaction.channel.fetch_message(vote_msg.id)
        except discord.HTTPException:
            log.error("Could not fetch unNaN vote message")
            return

        thumbs_up = 0
        thumbs_down = 0
        for reaction in vote_msg_updated.reactions:
            if str(reaction.emoji) == "\U0001f44d":
                thumbs_up = reaction.count - 1
            elif str(reaction.emoji) == "\U0001f44e":
                thumbs_down = reaction.count - 1

        if thumbs_up > thumbs_down:
            if not db.is_user_nan(guild_id, user_id):
                await interaction.channel.send(
                    f"Vote passed but {user.mention} is no longer NaN."
                )
                return
            count = db.delete_nan_transactions(guild_id, user_id)
            new_total = db.get_user_points(guild_id, user_id)
            await interaction.channel.send(
                f"Vote passed ({thumbs_up} - {thumbs_down}). "
                f"{user.mention} has been freed from NaN! "
                f"Removed {count} NaN transaction{'s' if count != 1 else ''}. "
                f"Points restored to **{_fmt_points(new_total)}**."
            )
        else:
            await interaction.channel.send(
                f"Vote failed ({thumbs_up} - {thumbs_down}). "
                f"{user.mention} remains **NaN**."
            )

    asyncio.create_task(tally())


@client.event
async def on_ready():
    db.init_db()
    await tree.sync(guild=guild_obj)
    log.info("Bot ready — logged in as %s", client.user)


client.run(BOT_TOKEN)
