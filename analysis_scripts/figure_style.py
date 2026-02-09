"""
Shared style configuration for publication-quality figures.
Designed for Nature-style publications.
"""
import matplotlib.pyplot as plt
import matplotlib as mpl
import numpy as np

DEFAULT_DB = "data/moltbook_paper.db"

# Nature-friendly color palette (colorblind-safe)
COLORS = {
    'blue': '#0077BB',
    'orange': '#EE7733',
    'green': '#009988',
    'red': '#CC3311',
    'purple': '#AA3377',
    'cyan': '#33BBEE',
    'grey': '#BBBBBB',
    'black': '#000000',
}

# Sequential colors for multiple series
COLOR_SEQUENCE = [
    '#0077BB',  # blue
    '#EE7733',  # orange
    '#009988',  # green
    '#CC3311',  # red
    '#AA3377',  # purple
    '#33BBEE',  # cyan
]

# Featured submolts to highlight
FEATURED_SUBMOLTS = [
    'announcements',
    'general',
    'introductions',
    'blesstheirhearts',
    'todayilearned',
]


def setup_style():
    """Configure matplotlib for publication-quality figures."""
    plt.rcParams.update({
        # Font settings
        'font.family': 'sans-serif',
        'font.sans-serif': ['Arial', 'Helvetica', 'DejaVu Sans'],
        'font.size': 10,
        'axes.labelsize': 11,
        'axes.titlesize': 12,
        'xtick.labelsize': 9,
        'ytick.labelsize': 9,
        'legend.fontsize': 9,

        # Line and marker settings
        'lines.linewidth': 1.5,
        'lines.markersize': 5,

        # Axes settings
        'axes.linewidth': 1.0,
        'axes.spines.top': False,
        'axes.spines.right': False,

        # Grid settings
        'axes.grid': False,
        'grid.alpha': 0.3,
        'grid.linewidth': 0.5,

        # Legend settings
        'legend.frameon': True,
        'legend.framealpha': 0.9,
        'legend.edgecolor': 'none',

        # Figure settings
        'figure.dpi': 150,
        'savefig.dpi': 300,
        'savefig.bbox': 'tight',
        'savefig.pad_inches': 0.1,
    })


def add_panel_label(ax, label, x=-0.12, y=1.08):
    """Add panel label (a, b, c, etc.) to subplot."""
    ax.text(x, y, label, transform=ax.transAxes, fontsize=14,
            fontweight='bold', va='top', ha='left')


def format_axis_log(ax, which='both'):
    """Format log-scale axes with minor ticks."""
    if which in ['x', 'both']:
        ax.set_xscale('log')
    if which in ['y', 'both']:
        ax.set_yscale('log')
    ax.grid(True, which='major', alpha=0.3, linewidth=0.5)
    ax.grid(True, which='minor', alpha=0.15, linewidth=0.3)


def compute_ccdf(data):
    """Compute complementary cumulative distribution function."""
    data = np.array(data)
    data = data[~np.isnan(data)]
    data = data[data > 0]
    if len(data) == 0:
        return np.array([]), np.array([])
    sorted_data = np.sort(data)
    n = len(sorted_data)
    ccdf = np.arange(n, 0, -1) / n
    return sorted_data, ccdf


def compute_pdf_logbins(data, num_bins=35):
    """Compute PDF using logarithmic binning."""
    data = np.array(data)
    data = data[data > 0]
    if len(data) < 10:
        return np.array([]), np.array([])

    log_bins = np.logspace(np.log10(data.min()), np.log10(data.max()), num_bins)
    counts, edges = np.histogram(data, bins=log_bins)
    bin_widths = edges[1:] - edges[:-1]
    bin_centers = np.sqrt(edges[:-1] * edges[1:])
    pdf = counts / (len(data) * bin_widths)

    mask = pdf > 0
    return bin_centers[mask], pdf[mask]


def fit_power_law(x, y, x_min=None, x_max=None):
    """Fit power law to data in log-log space. Returns (exponent, prefactor)."""
    mask = np.ones(len(x), dtype=bool)
    if x_min is not None:
        mask &= (x >= x_min)
    if x_max is not None:
        mask &= (x <= x_max)

    if mask.sum() < 3:
        return None, None

    log_x = np.log10(x[mask])
    log_y = np.log10(y[mask])
    coeffs = np.polyfit(log_x, log_y, 1)

    return coeffs[0], 10**coeffs[1]


def get_spam_post_ids(conn):
    """Get IDs of spam-flooded posts based on comment patterns."""
    spam_posts = conn.execute('''
        SELECT post_id
        FROM comments
        GROUP BY post_id
        HAVING COUNT(*) >= 5
        AND (
            COUNT(DISTINCT content) * 1.0 / COUNT(*) < 0.5
            OR COUNT(DISTINCT author_name) * 1.0 / COUNT(*) < 0.2
        )
    ''').fetchall()
    spam_ids = set(r[0] for r in spam_posts)

    # Also add high-count posts without stored comments
    high_count_no_comments = conn.execute('''
        SELECT p.id
        FROM posts p
        LEFT JOIN (SELECT DISTINCT post_id FROM comments) c ON p.id = c.post_id
        WHERE c.post_id IS NULL AND p.comment_count > 200
    ''').fetchall()
    spam_ids.update(r[0] for r in high_count_no_comments)

    return spam_ids
