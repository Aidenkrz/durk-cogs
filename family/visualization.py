import logging
import math
from io import BytesIO
from typing import TYPE_CHECKING, Dict, List, Set, Optional, Tuple

try:
    from PIL import Image, ImageDraw, ImageFont
    PILLOW_AVAILABLE = True
except ImportError:
    PILLOW_AVAILABLE = False

if TYPE_CHECKING:
    from .database import FamilyDatabase
    from redbot.core.bot import Red

log = logging.getLogger("red.DurkCogs.Family.visualization")


class FamilyTreeVisualizer:
    """Generates family tree visualizations using Pillow."""

    # Color scheme (RGB tuples for Pillow)
    COLORS = {
        'self': (255, 215, 0),       # Gold
        'spouse': (255, 105, 180),   # Hot pink
        'parent': (65, 105, 225),    # Royal blue
        'child': (50, 205, 50),      # Lime green
        'sibling': (255, 165, 0),    # Orange
        'extended': (147, 112, 219), # Medium purple
    }

    # Edge colors
    EDGE_COLORS = {
        'marriage': (255, 105, 180),   # Pink
        'parent_child': (65, 105, 225), # Blue
    }

    # Background color (Discord dark theme)
    BG_COLOR = (47, 49, 54)
    TEXT_COLOR = (255, 255, 255)

    def __init__(self):
        if not PILLOW_AVAILABLE:
            log.warning("Pillow not available. Tree visualization will be disabled.")

    @property
    def available(self) -> bool:
        """Check if visualization dependencies are available."""
        return PILLOW_AVAILABLE

    async def generate_tree(
        self,
        db: "FamilyDatabase",
        user_id: int,
        bot: "Red",
        depth: int = 2
    ) -> Optional[BytesIO]:
        """
        Generate a family tree image centered on user_id.

        Args:
            db: Database instance
            user_id: The central user to build the tree around
            bot: Discord bot instance for fetching usernames
            depth: How many relationship levels to traverse

        Returns:
            BytesIO containing PNG image, or None if unavailable
        """
        if not PILLOW_AVAILABLE:
            return None

        # Collect all family members and their relationships
        nodes: Dict[int, dict] = {}
        edges: List[Tuple[int, int, str]] = []
        visited: Set[int] = set()

        await self._collect_family(
            db, user_id, bot, nodes, edges, visited, depth, 0, user_id
        )

        if not nodes:
            return None

        # Calculate positions using a simple force-directed layout
        positions = self._calculate_positions(nodes, edges)

        # Determine image size based on positions
        min_x = min(p[0] for p in positions.values()) - 100
        max_x = max(p[0] for p in positions.values()) + 100
        min_y = min(p[1] for p in positions.values()) - 60
        max_y = max(p[1] for p in positions.values()) + 60

        width = max(600, int(max_x - min_x + 200))
        height = max(400, int(max_y - min_y + 150))

        # Offset positions to fit in image
        offset_x = -min_x + 100
        offset_y = -min_y + 80

        # Create image
        img = Image.new('RGB', (width, height), self.BG_COLOR)
        draw = ImageDraw.Draw(img)

        # Try to load a font, fall back to default
        try:
            font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 14)
            title_font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 18)
        except (OSError, IOError):
            try:
                font = ImageFont.truetype("/usr/share/fonts/TTF/DejaVuSans.ttf", 14)
                title_font = ImageFont.truetype("/usr/share/fonts/TTF/DejaVuSans-Bold.ttf", 18)
            except (OSError, IOError):
                font = ImageFont.load_default()
                title_font = font

        # Draw title
        central_name = nodes[user_id]['name']
        title = f"Family Tree for {central_name}"
        title_bbox = draw.textbbox((0, 0), title, font=title_font)
        title_width = title_bbox[2] - title_bbox[0]
        draw.text(((width - title_width) // 2, 15), title, fill=self.TEXT_COLOR, font=title_font)

        # Draw edges first (so they're behind nodes)
        for user1_id, user2_id, edge_type in edges:
            if user1_id in positions and user2_id in positions:
                x1, y1 = positions[user1_id]
                x2, y2 = positions[user2_id]
                x1, y1 = x1 + offset_x, y1 + offset_y
                x2, y2 = x2 + offset_x, y2 + offset_y

                color = self.EDGE_COLORS.get(edge_type, (128, 128, 128))

                if edge_type == 'marriage':
                    # Solid thick line for marriage
                    draw.line([(x1, y1), (x2, y2)], fill=color, width=3)
                else:
                    # Dashed line for parent-child
                    self._draw_dashed_line(draw, x1, y1, x2, y2, color, width=2)

        # Draw nodes
        node_radius = 35
        for uid, node_data in nodes.items():
            if uid not in positions:
                continue

            x, y = positions[uid]
            x, y = x + offset_x, y + offset_y

            color = self.COLORS.get(node_data['type'], self.COLORS['extended'])

            # Draw circle
            draw.ellipse(
                [(x - node_radius, y - node_radius),
                 (x + node_radius, y + node_radius)],
                fill=color,
                outline=self.TEXT_COLOR,
                width=2
            )

            # Draw name (truncate if needed)
            name = node_data['name']
            if len(name) > 12:
                name = name[:10] + ".."

            text_bbox = draw.textbbox((0, 0), name, font=font)
            text_width = text_bbox[2] - text_bbox[0]
            text_height = text_bbox[3] - text_bbox[1]

            draw.text(
                (x - text_width // 2, y - text_height // 2),
                name,
                fill=self.TEXT_COLOR,
                font=font
            )

        # Draw legend
        legend_y = height - 35
        legend_items = [
            ('You', self.COLORS['self']),
            ('Spouse', self.COLORS['spouse']),
            ('Parent', self.COLORS['parent']),
            ('Child', self.COLORS['child']),
            ('Sibling', self.COLORS['sibling']),
        ]

        legend_x = 20
        for label, color in legend_items:
            draw.ellipse([(legend_x, legend_y), (legend_x + 15, legend_y + 15)], fill=color)
            draw.text((legend_x + 20, legend_y), label, fill=self.TEXT_COLOR, font=font)
            legend_x += 90

        # Save to BytesIO
        buffer = BytesIO()
        img.save(buffer, format='PNG')
        buffer.seek(0)

        return buffer

    def _draw_dashed_line(self, draw, x1, y1, x2, y2, color, width=1, dash_length=8):
        """Draw a dashed line."""
        dx = x2 - x1
        dy = y2 - y1
        distance = math.sqrt(dx * dx + dy * dy)

        if distance == 0:
            return

        dashes = int(distance / dash_length)
        if dashes == 0:
            dashes = 1

        for i in range(0, dashes, 2):
            start = i / dashes
            end = min((i + 1) / dashes, 1)

            sx = x1 + dx * start
            sy = y1 + dy * start
            ex = x1 + dx * end
            ey = y1 + dy * end

            draw.line([(sx, sy), (ex, ey)], fill=color, width=width)

    def _calculate_positions(
        self, nodes: Dict[int, dict], edges: List[Tuple[int, int, str]]
    ) -> Dict[int, Tuple[float, float]]:
        """Calculate node positions using a simple force-directed layout."""
        if not nodes:
            return {}

        # Initialize positions in a circle
        positions = {}
        n = len(nodes)
        node_ids = list(nodes.keys())

        for i, uid in enumerate(node_ids):
            angle = 2 * math.pi * i / n
            positions[uid] = (math.cos(angle) * 150, math.sin(angle) * 150)

        # Simple force-directed iterations
        for _ in range(50):
            forces = {uid: [0.0, 0.0] for uid in node_ids}

            # Repulsion between all nodes
            for i, uid1 in enumerate(node_ids):
                for uid2 in node_ids[i + 1:]:
                    x1, y1 = positions[uid1]
                    x2, y2 = positions[uid2]

                    dx = x2 - x1
                    dy = y2 - y1
                    dist = math.sqrt(dx * dx + dy * dy) + 0.1

                    # Repulsion force
                    force = 5000 / (dist * dist)
                    fx = -force * dx / dist
                    fy = -force * dy / dist

                    forces[uid1][0] += fx
                    forces[uid1][1] += fy
                    forces[uid2][0] -= fx
                    forces[uid2][1] -= fy

            # Attraction along edges
            for uid1, uid2, _ in edges:
                if uid1 not in positions or uid2 not in positions:
                    continue

                x1, y1 = positions[uid1]
                x2, y2 = positions[uid2]

                dx = x2 - x1
                dy = y2 - y1
                dist = math.sqrt(dx * dx + dy * dy) + 0.1

                # Attraction force
                force = dist * 0.05
                fx = force * dx / dist
                fy = force * dy / dist

                forces[uid1][0] += fx
                forces[uid1][1] += fy
                forces[uid2][0] -= fx
                forces[uid2][1] -= fy

            # Apply forces with damping
            for uid in node_ids:
                x, y = positions[uid]
                fx, fy = forces[uid]

                # Limit force magnitude
                mag = math.sqrt(fx * fx + fy * fy)
                if mag > 10:
                    fx = fx / mag * 10
                    fy = fy / mag * 10

                positions[uid] = (x + fx, y + fy)

        return positions

    async def _collect_family(
        self,
        db: "FamilyDatabase",
        user_id: int,
        bot: "Red",
        nodes: Dict[int, dict],
        edges: List[Tuple[int, int, str]],
        visited: Set[int],
        max_depth: int,
        current_depth: int,
        central_user_id: int
    ):
        """Recursively collect family members and relationships."""
        if user_id in visited or current_depth > max_depth:
            return

        visited.add(user_id)

        # Get user name
        user = bot.get_user(user_id)
        display_name = user.display_name if user else f"User {user_id}"

        # Determine node type
        if user_id == central_user_id:
            node_type = 'self'
        else:
            node_type = 'extended'

        nodes[user_id] = {'name': display_name, 'type': node_type}

        # Get spouses
        spouses = await db.get_spouses(user_id)
        for spouse_id in spouses:
            if spouse_id not in nodes:
                spouse = bot.get_user(spouse_id)
                spouse_name = spouse.display_name if spouse else f"User {spouse_id}"
                nodes[spouse_id] = {'name': spouse_name, 'type': 'spouse'}

            # Add edge if not already added
            edge = tuple(sorted([user_id, spouse_id]))
            if not any(e[0] == edge[0] and e[1] == edge[1] for e in edges):
                edges.append((edge[0], edge[1], 'marriage'))

            if current_depth < max_depth:
                await self._collect_family(
                    db, spouse_id, bot, nodes, edges, visited,
                    max_depth, current_depth + 1, central_user_id
                )

        # Get parents
        parents = await db.get_parents(user_id)
        for parent_id in parents:
            if parent_id not in nodes:
                parent = bot.get_user(parent_id)
                parent_name = parent.display_name if parent else f"User {parent_id}"
                nodes[parent_id] = {'name': parent_name, 'type': 'parent'}

            edge = tuple(sorted([parent_id, user_id]))
            if not any(e[0] == edge[0] and e[1] == edge[1] for e in edges):
                edges.append((edge[0], edge[1], 'parent_child'))

            if current_depth < max_depth:
                await self._collect_family(
                    db, parent_id, bot, nodes, edges, visited,
                    max_depth, current_depth + 1, central_user_id
                )

        # Get children
        children = await db.get_children(user_id)
        for child_id in children:
            if child_id not in nodes:
                child = bot.get_user(child_id)
                child_name = child.display_name if child else f"User {child_id}"
                nodes[child_id] = {'name': child_name, 'type': 'child'}

            edge = tuple(sorted([user_id, child_id]))
            if not any(e[0] == edge[0] and e[1] == edge[1] for e in edges):
                edges.append((edge[0], edge[1], 'parent_child'))

            if current_depth < max_depth:
                await self._collect_family(
                    db, child_id, bot, nodes, edges, visited,
                    max_depth, current_depth + 1, central_user_id
                )

        # Get siblings
        siblings = await db.get_siblings(user_id)
        for sibling_id in siblings:
            if sibling_id not in nodes:
                sibling = bot.get_user(sibling_id)
                sibling_name = sibling.display_name if sibling else f"User {sibling_id}"
                nodes[sibling_id] = {'name': sibling_name, 'type': 'sibling'}


async def generate_text_tree(db: "FamilyDatabase", user_id: int, bot: "Red") -> str:
    """Generate a simple text-based family tree as fallback."""
    lines = []

    user = bot.get_user(user_id)
    user_name = user.display_name if user else f"User {user_id}"
    lines.append(f"**Family of {user_name}**\n")

    # Parents
    parents = await db.get_parents(user_id)
    if parents:
        lines.append("**Parents:**")
        for p in parents:
            parent = bot.get_user(p)
            name = parent.display_name if parent else f"User {p}"
            lines.append(f"  \u2514 {name}")

    # Spouses
    spouses = await db.get_spouses(user_id)
    if spouses:
        lines.append("**Spouses:**")
        for s in spouses:
            spouse = bot.get_user(s)
            name = spouse.display_name if spouse else f"User {s}"
            lines.append(f"  \u2764 {name}")

    # Siblings
    siblings = await db.get_siblings(user_id)
    if siblings:
        lines.append("**Siblings:**")
        for s in siblings:
            sibling = bot.get_user(s)
            name = sibling.display_name if sibling else f"User {s}"
            lines.append(f"  \u2194 {name}")

    # Children
    children = await db.get_children(user_id)
    if children:
        lines.append("**Children:**")
        for c in children:
            child = bot.get_user(c)
            name = child.display_name if child else f"User {c}"
            lines.append(f"  \u2514 {name}")

    if len(lines) == 1:
        lines.append("No family connections yet.")

    return "\n".join(lines)
