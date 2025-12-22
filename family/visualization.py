import logging
import math
from io import BytesIO
from typing import TYPE_CHECKING, Dict, List, Set, Optional, Tuple
from collections import defaultdict

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
    """Generates hierarchical family tree visualizations using Pillow."""

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
        'parent_child': (100, 149, 237), # Cornflower blue
    }

    # Background color (Discord dark theme)
    BG_COLOR = (54, 57, 63)

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
        Generate a hierarchical family tree image centered on user_id.
        """
        if not PILLOW_AVAILABLE:
            return None

        # Collect family data
        family_data = await self._collect_family_hierarchical(db, user_id, bot, depth)

        if not family_data['nodes']:
            return None

        # Calculate hierarchical positions
        positions = self._calculate_hierarchical_positions(family_data, user_id)

        if not positions:
            return None

        # Layout constants
        node_width = 120
        node_height = 50
        h_spacing = 160  # Horizontal spacing between nodes
        v_spacing = 120  # Vertical spacing between levels
        margin = 80

        # Calculate image dimensions
        all_x = [p[0] for p in positions.values()]
        all_y = [p[1] for p in positions.values()]

        min_x, max_x = min(all_x), max(all_x)
        min_y, max_y = min(all_y), max(all_y)

        width = int((max_x - min_x) * h_spacing + node_width + margin * 2)
        height = int((max_y - min_y) * v_spacing + node_height + margin * 2)

        # Ensure minimum size
        width = max(600, width)
        height = max(400, height)

        # Offset to center the tree
        offset_x = -min_x * h_spacing + margin + node_width // 2
        offset_y = -min_y * v_spacing + margin + node_height // 2

        # Create image
        img = Image.new('RGB', (width, height), self.BG_COLOR)
        draw = ImageDraw.Draw(img)

        # Load fonts
        try:
            font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 14)
            title_font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 20)
        except (OSError, IOError):
            try:
                font = ImageFont.truetype("/usr/share/fonts/TTF/DejaVuSans-Bold.ttf", 14)
                title_font = ImageFont.truetype("/usr/share/fonts/TTF/DejaVuSans-Bold.ttf", 20)
            except (OSError, IOError):
                font = ImageFont.load_default()
                title_font = font

        # Draw title
        central_name = family_data['nodes'][user_id]['name']
        title = f"Family Tree for {central_name}"
        title_bbox = draw.textbbox((0, 0), title, font=title_font)
        title_width = title_bbox[2] - title_bbox[0]
        draw.text(((width - title_width) // 2, 15), title, fill=(255, 255, 255), font=title_font)

        # Draw edges first
        for edge in family_data['edges']:
            uid1, uid2, edge_type = edge
            if uid1 in positions and uid2 in positions:
                x1 = positions[uid1][0] * h_spacing + offset_x
                y1 = positions[uid1][1] * v_spacing + offset_y
                x2 = positions[uid2][0] * h_spacing + offset_x
                y2 = positions[uid2][1] * v_spacing + offset_y

                color = self.EDGE_COLORS.get(edge_type, (128, 128, 128))

                if edge_type == 'marriage':
                    # Horizontal line for marriage (same level)
                    draw.line([(x1, y1), (x2, y2)], fill=color, width=3)
                else:
                    # Elbow connector for parent-child
                    mid_y = (y1 + y2) / 2
                    draw.line([(x1, y1), (x1, mid_y)], fill=color, width=2)
                    draw.line([(x1, mid_y), (x2, mid_y)], fill=color, width=2)
                    draw.line([(x2, mid_y), (x2, y2)], fill=color, width=2)

        # Draw nodes
        node_h = 45
        node_w = 110
        corner_radius = 8

        for uid, node_data in family_data['nodes'].items():
            if uid not in positions:
                continue

            x = positions[uid][0] * h_spacing + offset_x
            y = positions[uid][1] * v_spacing + offset_y

            color = self.COLORS.get(node_data['type'], self.COLORS['extended'])

            # Draw rounded rectangle
            self._draw_rounded_rect(
                draw,
                x - node_w // 2, y - node_h // 2,
                x + node_w // 2, y + node_h // 2,
                corner_radius,
                fill=color,
                outline=(255, 255, 255),
                width=2
            )

            # Draw name
            name = node_data['name']
            if len(name) > 14:
                name = name[:12] + ".."

            text_bbox = draw.textbbox((0, 0), name, font=font)
            text_width = text_bbox[2] - text_bbox[0]
            text_height = text_bbox[3] - text_bbox[1]

            # Use black text for light colors, white for dark
            brightness = (color[0] * 299 + color[1] * 587 + color[2] * 114) / 1000
            text_color = (0, 0, 0) if brightness > 128 else (255, 255, 255)

            draw.text(
                (x - text_width // 2, y - text_height // 2),
                name,
                fill=text_color,
                font=font
            )

        # Draw legend at bottom
        legend_y = height - 40
        legend_items = [
            ('You', self.COLORS['self']),
            ('Spouse', self.COLORS['spouse']),
            ('Parent', self.COLORS['parent']),
            ('Child', self.COLORS['child']),
            ('Sibling', self.COLORS['sibling']),
        ]

        legend_x = 30
        for label, color in legend_items:
            draw.ellipse([(legend_x, legend_y), (legend_x + 16, legend_y + 16)],
                        fill=color, outline=(255, 255, 255), width=1)
            draw.text((legend_x + 22, legend_y), label, fill=(255, 255, 255), font=font)
            legend_x += 100

        # Save to BytesIO
        buffer = BytesIO()
        img.save(buffer, format='PNG', quality=95)
        buffer.seek(0)

        return buffer

    def _draw_rounded_rect(self, draw, x1, y1, x2, y2, radius, fill, outline, width):
        """Draw a rounded rectangle."""
        # Draw the main rectangle parts
        draw.rectangle([x1 + radius, y1, x2 - radius, y2], fill=fill)
        draw.rectangle([x1, y1 + radius, x2, y2 - radius], fill=fill)

        # Draw the four corners
        draw.ellipse([x1, y1, x1 + radius * 2, y1 + radius * 2], fill=fill)
        draw.ellipse([x2 - radius * 2, y1, x2, y1 + radius * 2], fill=fill)
        draw.ellipse([x1, y2 - radius * 2, x1 + radius * 2, y2], fill=fill)
        draw.ellipse([x2 - radius * 2, y2 - radius * 2, x2, y2], fill=fill)

        # Draw outline
        if outline and width > 0:
            # Top and bottom edges
            draw.line([(x1 + radius, y1), (x2 - radius, y1)], fill=outline, width=width)
            draw.line([(x1 + radius, y2), (x2 - radius, y2)], fill=outline, width=width)
            # Left and right edges
            draw.line([(x1, y1 + radius), (x1, y2 - radius)], fill=outline, width=width)
            draw.line([(x2, y1 + radius), (x2, y2 - radius)], fill=outline, width=width)
            # Corner arcs (approximated)
            draw.arc([x1, y1, x1 + radius * 2, y1 + radius * 2], 180, 270, fill=outline, width=width)
            draw.arc([x2 - radius * 2, y1, x2, y1 + radius * 2], 270, 360, fill=outline, width=width)
            draw.arc([x1, y2 - radius * 2, x1 + radius * 2, y2], 90, 180, fill=outline, width=width)
            draw.arc([x2 - radius * 2, y2 - radius * 2, x2, y2], 0, 90, fill=outline, width=width)

    def _calculate_hierarchical_positions(
        self, family_data: dict, central_user_id: int
    ) -> Dict[int, Tuple[float, float]]:
        """
        Calculate hierarchical positions with:
        - Central user at (0, 0)
        - Spouses horizontally adjacent
        - Parents above (negative y)
        - Children below (positive y)
        """
        nodes = family_data['nodes']
        edges = family_data['edges']
        levels = family_data['levels']

        if not nodes:
            return {}

        positions = {}

        # Group nodes by level
        level_nodes = defaultdict(list)
        for uid, level in levels.items():
            level_nodes[level].append(uid)

        # Sort levels
        sorted_levels = sorted(level_nodes.keys())

        # Position nodes at each level
        for level in sorted_levels:
            nodes_at_level = level_nodes[level]

            # Sort nodes to keep spouses together and central user in middle
            if level == 0:
                # Put central user first, then their spouses
                sorted_nodes = [central_user_id] if central_user_id in nodes_at_level else []
                for uid in nodes_at_level:
                    if uid != central_user_id:
                        sorted_nodes.append(uid)
                nodes_at_level = sorted_nodes

            n = len(nodes_at_level)
            # Center the nodes at this level
            start_x = -(n - 1) / 2

            for i, uid in enumerate(nodes_at_level):
                positions[uid] = (start_x + i, level)

        return positions

    async def _collect_family_hierarchical(
        self,
        db: "FamilyDatabase",
        user_id: int,
        bot: "Red",
        depth: int
    ) -> dict:
        """
        Collect family members with level information for hierarchical layout.
        Level 0 = central user and spouses
        Level -1, -2 = parents, grandparents (above)
        Level 1, 2 = children, grandchildren (below)
        """
        nodes: Dict[int, dict] = {}
        edges: List[Tuple[int, int, str]] = []
        levels: Dict[int, int] = {}
        visited: Set[int] = set()

        async def get_name(uid: int) -> str:
            user = bot.get_user(uid)
            return user.display_name if user else f"User {uid}"

        async def collect_ancestors(uid: int, current_level: int, max_level: int):
            """Collect parents and grandparents (going up)."""
            if uid in visited or current_level < -max_level:
                return

            parents = await db.get_parents(uid)
            for parent_id in parents:
                if parent_id not in nodes:
                    nodes[parent_id] = {
                        'name': await get_name(parent_id),
                        'type': 'parent'
                    }
                    levels[parent_id] = current_level - 1

                edge = tuple(sorted([parent_id, uid]))
                if not any(e[0] == edge[0] and e[1] == edge[1] for e in edges):
                    edges.append((parent_id, uid, 'parent_child'))

                await collect_ancestors(parent_id, current_level - 1, max_level)

        async def collect_descendants(uid: int, current_level: int, max_level: int):
            """Collect children and grandchildren (going down)."""
            if uid in visited or current_level > max_level:
                return

            children = await db.get_children(uid)
            for child_id in children:
                if child_id not in nodes:
                    nodes[child_id] = {
                        'name': await get_name(child_id),
                        'type': 'child'
                    }
                    levels[child_id] = current_level + 1

                edge = tuple(sorted([uid, child_id]))
                if not any(e[0] == edge[0] and e[1] == edge[1] for e in edges):
                    edges.append((uid, child_id, 'parent_child'))

                await collect_descendants(child_id, current_level + 1, max_level)

        # Add central user
        nodes[user_id] = {
            'name': await get_name(user_id),
            'type': 'self'
        }
        levels[user_id] = 0

        # Add spouses at same level
        spouses = await db.get_spouses(user_id)
        for spouse_id in spouses:
            nodes[spouse_id] = {
                'name': await get_name(spouse_id),
                'type': 'spouse'
            }
            levels[spouse_id] = 0
            edges.append((user_id, spouse_id, 'marriage'))

        # Add siblings at same level
        siblings = await db.get_siblings(user_id)
        for sibling_id in siblings:
            if sibling_id not in nodes:
                nodes[sibling_id] = {
                    'name': await get_name(sibling_id),
                    'type': 'sibling'
                }
                levels[sibling_id] = 0

        # Collect ancestors (parents, grandparents)
        await collect_ancestors(user_id, 0, depth)

        # Collect descendants (children, grandchildren)
        await collect_descendants(user_id, 0, depth)

        # Also collect descendants of spouses
        for spouse_id in spouses:
            await collect_descendants(spouse_id, 0, depth)

        return {
            'nodes': nodes,
            'edges': edges,
            'levels': levels
        }


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
            lines.append(f"  └ {name}")

    # Spouses
    spouses = await db.get_spouses(user_id)
    if spouses:
        lines.append("**Spouses:**")
        for s in spouses:
            spouse = bot.get_user(s)
            name = spouse.display_name if spouse else f"User {s}"
            lines.append(f"  ♥ {name}")

    # Siblings
    siblings = await db.get_siblings(user_id)
    if siblings:
        lines.append("**Siblings:**")
        for s in siblings:
            sibling = bot.get_user(s)
            name = sibling.display_name if sibling else f"User {s}"
            lines.append(f"  ↔ {name}")

    # Children
    children = await db.get_children(user_id)
    if children:
        lines.append("**Children:**")
        for c in children:
            child = bot.get_user(c)
            name = child.display_name if child else f"User {c}"
            lines.append(f"  └ {name}")

    if len(lines) == 1:
        lines.append("No family connections yet.")

    return "\n".join(lines)
