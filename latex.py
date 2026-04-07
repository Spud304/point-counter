"""LaTeX rendering for math expressions and point values."""
import ast as _ast
import io
import logging
from decimal import Decimal
from fractions import Fraction

log = logging.getLogger("point-counter")

MAX_DISPLAY_LEN = 100


def needs_latex(val: Decimal | Fraction) -> bool:
    """Return True if the value would benefit from LaTeX rendering."""
    if isinstance(val, Fraction):
        if val.denominator == 1:
            return len(str(val.numerator)) > MAX_DISPLAY_LEN
        # Terminating decimals don't need LaTeX unless they'd be sci notation
        d = val.denominator
        while d % 2 == 0:
            d //= 2
        while d % 5 == 0:
            d //= 5
        if d == 1:
            dec = Decimal(val.numerator) / Decimal(val.denominator)
            return needs_latex(dec)
        return True  # True fraction
    if isinstance(val, Decimal):
        if val.is_nan() or val.is_infinite():
            return True
        sign, digits, exponent = val.normalize().as_tuple()
        num_digits = len(digits) + max(exponent, 0) if isinstance(exponent, int) else 0
        if num_digits > MAX_DISPLAY_LEN:
            return True
        if val == val.to_integral_value():
            return False
        text = format(val.normalize(), 'f')
        return len(text) > MAX_DISPLAY_LEN
    return False


def _val_to_latex(val: Decimal | Fraction) -> str:
    """Convert a point value to a LaTeX string."""
    if isinstance(val, Fraction):
        if val.denominator == 1:
            s = str(val.numerator)
            if len(s) > MAX_DISPLAY_LEN:
                return _sci_to_latex(Decimal(val.numerator))
            return s
        frac_str = f"{abs(val.numerator)}/{val.denominator}"
        if len(frac_str) > MAX_DISPLAY_LEN:
            return _sci_to_latex(Decimal(val.numerator) / Decimal(val.denominator))
        if val < 0:
            return f"-\\frac{{{abs(val.numerator)}}}{{{val.denominator}}}"
        return f"\\frac{{{val.numerator}}}{{{val.denominator}}}"

    if isinstance(val, Decimal):
        if val.is_nan():
            return r"\mathrm{NaN}"
        if val.is_infinite():
            return r"-\infty" if val.is_signed() else r"\infty"
        sign, digits, exponent = val.normalize().as_tuple()
        num_digits = len(digits) + max(exponent, 0) if isinstance(exponent, int) else 0
        if num_digits > MAX_DISPLAY_LEN:
            return _sci_to_latex(val)
        if val == val.to_integral_value():
            return str(int(val))
        text = format(val.normalize(), 'f')
        if len(text) > MAX_DISPLAY_LEN:
            return _sci_to_latex(val)
        return text

    return str(val)


def _sci_to_latex(val: Decimal) -> str:
    """Convert a Decimal to scientific notation LaTeX."""
    s = f"{val.normalize():E}"
    if 'E' in s:
        mantissa, exp = s.split('E')
        exp = exp.lstrip('+')
        return f"{mantissa} \\times 10^{{{exp}}}"
    return s


# AST precedence levels
_PREC_ATOM = 100
_PREC_POW = 4
_PREC_UNARY = 3
_PREC_MUL = 2
_PREC_ADD = 1


def _node_prec(node) -> int:
    """Return the precedence of an AST node."""
    if isinstance(node, _ast.BinOp):
        if isinstance(node.op, _ast.Pow):
            return _PREC_POW
        if isinstance(node.op, (_ast.Mult, _ast.Div, _ast.Mod)):
            return _PREC_MUL
        return _PREC_ADD
    if isinstance(node, _ast.UnaryOp):
        return _PREC_UNARY
    return _PREC_ATOM


def _node_to_latex(node) -> str:
    """Convert an AST node to a LaTeX string."""
    if isinstance(node, _ast.Constant):
        if isinstance(node.value, (int, float)):
            v = node.value
            if isinstance(v, float) and v == int(v):
                return str(int(v))
            return str(v)
        return str(node.value)

    if isinstance(node, _ast.Name):
        names = {'p': 'p', 'e': 'e', 'pi': r'\pi', 'inf': r'\infty'}
        return names.get(node.id, rf'\mathrm{{{node.id}}}')

    if isinstance(node, _ast.UnaryOp):
        operand = _node_to_latex(node.operand)
        if isinstance(node.op, _ast.USub):
            if _node_prec(node.operand) < _PREC_UNARY:
                operand = f"\\left({operand}\\right)"
            return f"-{operand}"
        if isinstance(node.op, _ast.UAdd):
            return operand
        return operand

    if isinstance(node, _ast.BinOp):
        left = _node_to_latex(node.left)
        right = _node_to_latex(node.right)

        if isinstance(node.op, _ast.Add):
            return f"{left} + {right}"

        if isinstance(node.op, _ast.Sub):
            if _node_prec(node.right) <= _PREC_ADD:
                right = f"\\left({right}\\right)"
            return f"{left} - {right}"

        if isinstance(node.op, _ast.Mult):
            if _node_prec(node.left) < _PREC_MUL:
                left = f"\\left({left}\\right)"
            if _node_prec(node.right) < _PREC_MUL:
                right = f"\\left({right}\\right)"
            return f"{left} \\cdot {right}"

        if isinstance(node.op, _ast.Div):
            return f"\\frac{{{left}}}{{{right}}}"

        if isinstance(node.op, _ast.Pow):
            if isinstance(node.left, (_ast.BinOp, _ast.UnaryOp)):
                left = f"\\left({left}\\right)"
            return f"{{{left}}}^{{{right}}}"

        if isinstance(node.op, _ast.Mod):
            if _node_prec(node.left) < _PREC_MUL:
                left = f"\\left({left}\\right)"
            if _node_prec(node.right) < _PREC_MUL:
                right = f"\\left({right}\\right)"
            return f"{left} \\% {right}"

    if isinstance(node, _ast.Call):
        if not isinstance(node.func, _ast.Name):
            return r'\mathrm{?}'
        name = node.func.id
        args = [_node_to_latex(a) for a in node.args]

        if name == 'sqrt' and len(args) == 1:
            return f"\\sqrt{{{args[0]}}}"
        if name == 'abs' and len(args) == 1:
            return f"\\left|{args[0]}\\right|"
        if name == 'floor' and len(args) == 1:
            return f"\\lfloor {args[0]} \\rfloor"
        if name == 'ceil' and len(args) == 1:
            return f"\\lceil {args[0]} \\rceil"
        if name in ('sin', 'cos', 'tan') and len(args) == 1:
            return f"\\{name}\\left({args[0]}\\right)"
        if name in ('sinh', 'cosh', 'tanh') and len(args) == 1:
            return f"\\mathrm{{{name}}}\\left({args[0]}\\right)"
        if name in ('asin', 'acos', 'atan') and len(args) == 1:
            latex_name = {'asin': 'arcsin', 'acos': 'arccos', 'atan': 'arctan'}[name]
            return f"\\mathrm{{{latex_name}}}\\left({args[0]}\\right)"
        if name == 'log':
            if len(args) == 1:
                return f"\\ln\\left({args[0]}\\right)"
            if len(args) == 2:
                return f"\\log_{{{args[1]}}}\\left({args[0]}\\right)"
        if name == 'log10' and len(args) == 1:
            return f"\\log_{{10}}\\left({args[0]}\\right)"
        if name == 'exp' and len(args) == 1:
            return f"e^{{{args[0]}}}"
        if name == 'sign' and len(args) == 1:
            return f"\\mathrm{{sgn}}\\left({args[0]}\\right)"
        if name == 'round' and len(args) == 1:
            return f"\\mathrm{{round}}\\left({args[0]}\\right)"
        if name == 'factorial' and len(args) == 1:
            if _node_prec(node.args[0]) < _PREC_ATOM:
                return f"\\left({args[0]}\\right)!"
            return f"{args[0]}!"
        if name == 'gamma' and len(args) == 1:
            return f"\\Gamma\\left({args[0]}\\right)"
        if name == 'fibonacci' and len(args) == 1:
            return f"F_{{{args[0]}}}"
        if name == 'totient' and len(args) == 1:
            return f"\\phi\\left({args[0]}\\right)"
        if name == 'nextprime' and len(args) == 1:
            return f"\\mathrm{{nextprime}}\\left({args[0]}\\right)"
        if name == 'choose' and len(args) == 2:
            return f"\\binom{{{args[0]}}}{{{args[1]}}}"
        if name == 'mod' and len(args) == 2:
            return f"{args[0]} \\% {args[1]}"
        if name == 'diff' and len(args) == 1:
            return f"\\frac{{d}}{{dp}}\\left({args[0]}\\right)"
        if name == 'integrate' and len(args) == 3:
            return f"\\int_{{{args[1]}}}^{{{args[2]}}} {args[0]} \\, dp"
        if name == 'lim' and len(args) == 2:
            return f"\\lim_{{p \\to {args[1]}}} {args[0]}"

        # Generic function fallback
        joined = ", ".join(args)
        return f"\\mathrm{{{name}}}\\left({joined}\\right)"

    return r'\mathrm{?}'


def _expr_to_latex(expr_str: str) -> str:
    """Convert an expression string to LaTeX."""
    expr_str = expr_str.replace('^', '**')
    try:
        tree = _ast.parse(expr_str, mode='eval')
        return _node_to_latex(tree.body)
    except Exception:
        return rf'\mathrm{{{expr_str}}}'


def to_latex(old=None, new=None, reason=None) -> str | None:
    """Compose a full LaTeX string for a point change.

    Returns None if no LaTeX rendering is warranted.
    """
    expr_part = None
    mult_part = None

    if reason:
        if reason.startswith('='):
            core = reason[1:]
            if ': ' in core:
                expr_part = core.split(': ', 1)[0]
            else:
                expr_part = core
        elif reason.startswith('x'):
            core = reason[1:]
            if ': ' in core:
                mult_part = core.split(': ', 1)[0]
            else:
                mult_part = core

    old_interesting = old is not None and needs_latex(old)
    new_interesting = new is not None and needs_latex(new)
    has_expr = expr_part is not None
    has_mult = mult_part is not None

    if not (old_interesting or new_interesting or has_expr or has_mult):
        return None

    if has_expr and old is not None and new is not None:
        expr_latex = _expr_to_latex(expr_part)
        return (
            f"{_val_to_latex(old)} \\to {_val_to_latex(new)}"
            f" \\quad f(p) = {expr_latex}"
        )

    if has_mult and old is not None and new is not None:
        return f"{_val_to_latex(old)} \\times {mult_part} = {_val_to_latex(new)}"

    if old is not None and new is not None:
        return f"{_val_to_latex(old)} \\to {_val_to_latex(new)}"

    if new is not None and new_interesting:
        return _val_to_latex(new)

    if old is not None and old_interesting:
        return _val_to_latex(old)

    return None


_mpl_loaded = False


def _ensure_mpl():
    global _mpl_loaded
    if not _mpl_loaded:
        import matplotlib
        matplotlib.use('Agg')
        _mpl_loaded = True


def render_latex(latex_str: str, fontsize: int = 20, dpi: int = 150):
    """Render a LaTeX string to a discord.File.

    Returns discord.File or None on failure. White text on transparent
    background for Discord dark theme.
    """
    try:
        _ensure_mpl()
        from matplotlib.backends.backend_agg import FigureCanvasAgg
        from matplotlib.figure import Figure

        import discord

        fig = Figure()
        FigureCanvasAgg(fig)
        fig.patch.set_alpha(0)
        fig.text(0.5, 0.5, f"${latex_str}$", fontsize=fontsize, color='white')

        buf = io.BytesIO()
        fig.savefig(
            buf, format='png', dpi=dpi, transparent=True,
            bbox_inches='tight', pad_inches=0.15,
        )
        del fig
        buf.seek(0)

        return discord.File(buf, filename="math.png")
    except Exception:
        log.exception("LaTeX render failed")
        return None
