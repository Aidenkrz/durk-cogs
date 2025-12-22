import logging
from io import BytesIO
from typing import TYPE_CHECKING, Dict, Set, Optional

try:
    import networkx as nx
    import matplotlib
    matplotlib.use('Agg')  # Use non-interactive backend
    import matplotlib.pyplot as plt
    import matplotlib.patches as mpatches
    VISUALIZATION_AVAILABLE = True
except ImportError:
    VISUALIZATION_AVAILABLE = False

if TYPE_CHECKING:
    from .database import FamilyDatabase
    from redbot.core.bot import Red

log = logging.getLogger("red.DurkCogs.Family.visualization")


class FamilyTreeVisualizer:
    """Generates network graph visualizations of family trees."""

    # Color scheme for different relationship types
    COLORS = {
        'self': '#FFD700',       # Gold for the focused user
        'spouse': '#FF69B4',     # Hot pink for spouses
        'parent': '#4169E1',     # Royal blue for parents
        'child': '#32CD32',      # Lime green for children
        'sibling': '#FFA500',    # Orange for siblings
        'extended': '#9370DB',   # Medium purple for extended family
    }

    # Edge colors
    EDGE_COLORS = {
        'marriage': '#FF69B4',   # Pink for marriages
        'parent_child': '#4169E1',  # Blue for parent-child
    }

    def __init__(self):
        if not VISUALIZATION_AVAILABLE:
            log.warning("NetworkX or Matplotlib not available. Tree visualization will be disabled.")

    @property
    def available(self) -> bool:
        """Check if visualization dependencies are available."""
        return VISUALIZATION_AVAILABLE

    async def generate_tree(
        self,
        db: "FamilyDatabase",
        user_id: int,
        bot: "Red",
        depth: int = 2
    ) -> Optional[BytesIO]:
        """
        Generate a family tree network graph centered on user_id.

        Args:
            db: Database instance
            user_id: The central user to build the tree around
            bot: Discord bot instance for fetching usernames
            depth: How many relationship levels to traverse

        Returns:
            BytesIO containing PNG image, or None if visualization unavailable
        """
        if not VISUALIZATION_AVAILABLE:
            return None

        G = nx.Graph()
        node_colors: Dict[int, str] = {}
        node_labels: Dict[int, str] = {}
        edge_types: Dict[tuple, str] = {}
        visited: Set[int] = set()

        # Build the graph
        await self._build_graph(
            G, db, user_id, bot,
            node_colors, node_labels, edge_types,
            depth, visited, 0, user_id
        )

        if len(G.nodes()) == 0:
            return None

        # Create figure with dark background for better Discord appearance
        fig, ax = plt.subplots(figsize=(14, 10), facecolor='#2C2F33')
        ax.set_facecolor('#2C2F33')

        # Use spring layout (force-directed) for network graph
        if len(G.nodes()) == 1:
            pos = {list(G.nodes())[0]: (0, 0)}
        else:
            pos = nx.spring_layout(G, k=2.5, iterations=50, seed=42)

        # Prepare node colors list
        colors = [node_colors.get(node, self.COLORS['extended']) for node in G.nodes()]

        # Draw nodes
        nx.draw_networkx_nodes(
            G, pos,
            node_color=colors,
            node_size=3000,
            ax=ax,
            edgecolors='white',
            linewidths=2
        )

        # Draw labels with white text
        labels = {node: node_labels.get(node, str(node)) for node in G.nodes()}
        nx.draw_networkx_labels(
            G, pos, labels,
            font_size=9,
            font_color='white',
            font_weight='bold',
            ax=ax
        )

        # Separate edges by type
        marriage_edges = [(u, v) for (u, v), t in edge_types.items() if t == 'marriage']
        parent_edges = [(u, v) for (u, v), t in edge_types.items() if t == 'parent_child']

        # Draw marriage edges (solid, thicker)
        if marriage_edges:
            nx.draw_networkx_edges(
                G, pos,
                edgelist=marriage_edges,
                edge_color=self.EDGE_COLORS['marriage'],
                width=3,
                style='solid',
                ax=ax
            )

        # Draw parent-child edges (dashed)
        if parent_edges:
            nx.draw_networkx_edges(
                G, pos,
                edgelist=parent_edges,
                edge_color=self.EDGE_COLORS['parent_child'],
                width=2,
                style='dashed',
                ax=ax
            )

        # Add legend
        legend_elements = [
            mpatches.Patch(color=self.COLORS['self'], label='You'),
            mpatches.Patch(color=self.COLORS['spouse'], label='Spouse'),
            mpatches.Patch(color=self.COLORS['parent'], label='Parent'),
            mpatches.Patch(color=self.COLORS['child'], label='Child'),
            mpatches.Patch(color=self.COLORS['sibling'], label='Sibling'),
            mpatches.Patch(color=self.COLORS['extended'], label='Extended'),
        ]

        # Add edge legend
        from matplotlib.lines import Line2D
        legend_elements.extend([
            Line2D([0], [0], color=self.EDGE_COLORS['marriage'], linewidth=3,
                   label='Marriage', linestyle='solid'),
            Line2D([0], [0], color=self.EDGE_COLORS['parent_child'], linewidth=2,
                   label='Parent/Child', linestyle='dashed'),
        ])

        ax.legend(
            handles=legend_elements,
            loc='upper left',
            facecolor='#23272A',
            edgecolor='white',
            labelcolor='white',
            fontsize=9
        )

        # Get the central user's name for the title
        central_name = node_labels.get(user_id, "Unknown")
        ax.set_title(
            f"Family Tree for {central_name}",
            fontsize=16,
            fontweight='bold',
            color='white',
            pad=20
        )

        ax.axis('off')

        # Adjust layout
        plt.tight_layout()

        # Save to BytesIO
        buffer = BytesIO()
        plt.savefig(
            buffer,
            format='png',
            dpi=150,
            bbox_inches='tight',
            facecolor='#2C2F33',
            edgecolor='none'
        )
        buffer.seek(0)
        plt.close(fig)

        return buffer

    async def _build_graph(
        self,
        G: "nx.Graph",
        db: "FamilyDatabase",
        user_id: int,
        bot: "Red",
        node_colors: Dict[int, str],
        node_labels: Dict[int, str],
        edge_types: Dict[tuple, str],
        max_depth: int,
        visited: Set[int],
        current_depth: int,
        central_user_id: int
    ):
        """Recursively build the graph from the database."""
        if user_id in visited or current_depth > max_depth:
            return

        visited.add(user_id)

        # Fetch user name
        user = bot.get_user(user_id)
        display_name = user.display_name if user else f"User {user_id}"
        # Truncate long names
        if len(display_name) > 15:
            display_name = display_name[:12] + "..."

        G.add_node(user_id)
        node_labels[user_id] = display_name

        # Color based on relationship to central user
        if user_id == central_user_id:
            node_colors[user_id] = self.COLORS['self']
        elif current_depth == 0:
            node_colors[user_id] = self.COLORS['self']

        # Fetch and add spouses
        spouses = await db.get_spouses(user_id)
        for spouse_id in spouses:
            if spouse_id not in visited:
                spouse = bot.get_user(spouse_id)
                spouse_name = spouse.display_name if spouse else f"User {spouse_id}"
                if len(spouse_name) > 15:
                    spouse_name = spouse_name[:12] + "..."

                G.add_node(spouse_id)
                node_labels[spouse_id] = spouse_name

                if spouse_id not in node_colors:
                    node_colors[spouse_id] = self.COLORS['spouse']

            # Add marriage edge
            edge_key = tuple(sorted([user_id, spouse_id]))
            if edge_key not in edge_types:
                G.add_edge(user_id, spouse_id)
                edge_types[edge_key] = 'marriage'

            # Recurse to spouse's family
            if current_depth < max_depth:
                await self._build_graph(
                    G, db, spouse_id, bot,
                    node_colors, node_labels, edge_types,
                    max_depth, visited, current_depth + 1, central_user_id
                )

        # Fetch and add parents
        parents = await db.get_parents(user_id)
        for parent_id in parents:
            if parent_id not in visited:
                parent = bot.get_user(parent_id)
                parent_name = parent.display_name if parent else f"User {parent_id}"
                if len(parent_name) > 15:
                    parent_name = parent_name[:12] + "..."

                G.add_node(parent_id)
                node_labels[parent_id] = parent_name

                if parent_id not in node_colors:
                    node_colors[parent_id] = self.COLORS['parent']

            # Add parent-child edge
            edge_key = tuple(sorted([parent_id, user_id]))
            if edge_key not in edge_types:
                G.add_edge(parent_id, user_id)
                edge_types[edge_key] = 'parent_child'

            # Recurse to parent's family
            if current_depth < max_depth:
                await self._build_graph(
                    G, db, parent_id, bot,
                    node_colors, node_labels, edge_types,
                    max_depth, visited, current_depth + 1, central_user_id
                )

        # Fetch and add children
        children = await db.get_children(user_id)
        for child_id in children:
            if child_id not in visited:
                child = bot.get_user(child_id)
                child_name = child.display_name if child else f"User {child_id}"
                if len(child_name) > 15:
                    child_name = child_name[:12] + "..."

                G.add_node(child_id)
                node_labels[child_id] = child_name

                if child_id not in node_colors:
                    node_colors[child_id] = self.COLORS['child']

            # Add parent-child edge
            edge_key = tuple(sorted([user_id, child_id]))
            if edge_key not in edge_types:
                G.add_edge(user_id, child_id)
                edge_types[edge_key] = 'parent_child'

            # Recurse to child's family
            if current_depth < max_depth:
                await self._build_graph(
                    G, db, child_id, bot,
                    node_colors, node_labels, edge_types,
                    max_depth, visited, current_depth + 1, central_user_id
                )

        # Fetch and add siblings (don't recurse deep into siblings)
        siblings = await db.get_siblings(user_id)
        for sibling_id in siblings:
            if sibling_id not in visited and sibling_id not in G.nodes():
                sibling = bot.get_user(sibling_id)
                sibling_name = sibling.display_name if sibling else f"User {sibling_id}"
                if len(sibling_name) > 15:
                    sibling_name = sibling_name[:12] + "..."

                G.add_node(sibling_id)
                node_labels[sibling_id] = sibling_name

                if sibling_id not in node_colors:
                    node_colors[sibling_id] = self.COLORS['sibling']

                # Connect siblings through their shared parent (already added)
                # No direct edge needed as they're connected via parent
