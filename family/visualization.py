import logging
import math
from io import BytesIO
from typing import TYPE_CHECKING, Dict, List, Set, Optional, Tuple
from collections import defaultdict

try:
    from PIL import Image, ImageDraw, ImageFont, ImageFilter
    PILLOW_AVAILABLE = True
except ImportError:
    PILLOW_AVAILABLE = False

if TYPE_CHECKING:
    from .database import FamilyDatabase
    from redbot.core.bot import Red

log = logging.getLogger("red.DurkCogs.Family.visualization")


class FamilyTreeVisualizer:
    """Generates beautiful hierarchical family tree visualizations using Pillow."""

    # Modern color palette with gradients (main color, darker shade for depth)
    COLORS = {
        'self': ((255, 200, 87), (230, 175, 60)),        # Warm gold
        'spouse': ((255, 130, 180), (230, 100, 155)),    # Soft pink
        'parent': ((100, 140, 230), (70, 110, 200)),     # Soft blue
        'child': ((120, 220, 120), (90, 190, 90)),       # Fresh green
        'sibling': ((255, 180, 100), (230, 155, 75)),    # Warm orange
        'in_law': ((180, 130, 255), (150, 100, 230)),    # Soft purple
        'grandparent': ((130, 170, 220), (100, 140, 190)), # Light steel blue
        'grandchild': ((170, 240, 170), (140, 210, 140)), # Light green
        'extended': ((180, 160, 230), (150, 130, 200)),  # Lavender
    }

    # Edge colors with transparency feel
    EDGE_COLORS = {
        'marriage': (255, 150, 190),     # Soft pink
        'parent_child': (140, 180, 230), # Soft blue
    }

    # Modern dark background (slightly blue-tinted)
    BG_COLOR = (32, 34, 37)
    BG_GRADIENT_TOP = (40, 44, 52)
    BG_GRADIENT_BOTTOM = (28, 30, 34)

    # Scale factor for high-res output
    SCALE = 2

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
        """Generate a beautiful hierarchical family tree image."""
        if not PILLOW_AVAILABLE:
            return None

        # Collect family data
        family_data = await self._collect_family_hierarchical(db, user_id, bot, depth)

        if not family_data['nodes']:
            return None

        # Calculate hierarchical positions with smart spouse grouping
        positions = self._calculate_smart_positions(family_data, user_id)

        if not positions:
            return None

        # Layout constants (will be scaled)
        node_width = 130
        node_height = 44
        h_spacing = 170
        v_spacing = 100
        margin = 100

        # Calculate image dimensions
        all_x = [p[0] for p in positions.values()]
        all_y = [p[1] for p in positions.values()]

        min_x, max_x = min(all_x), max(all_x)
        min_y, max_y = min(all_y), max(all_y)

        base_width = int((max_x - min_x) * h_spacing + node_width + margin * 2)
        base_height = int((max_y - min_y) * v_spacing + node_height + margin * 2 + 60)

        # Ensure minimum size
        base_width = max(650, base_width)
        base_height = max(450, base_height)

        # Scale up for high-res
        width = base_width * self.SCALE
        height = base_height * self.SCALE
        h_spacing *= self.SCALE
        v_spacing *= self.SCALE
        node_width *= self.SCALE
        node_height *= self.SCALE
        margin *= self.SCALE

        # Offset to center the tree
        offset_x = -min_x * h_spacing + margin + node_width // 2
        offset_y = -min_y * v_spacing + margin + node_height // 2 + 50 * self.SCALE

        # Create image with gradient background
        img = Image.new('RGB', (width, height), self.BG_COLOR)
        draw = ImageDraw.Draw(img)

        # Draw subtle gradient background
        for y in range(height):
            ratio = y / height
            r = int(self.BG_GRADIENT_TOP[0] * (1 - ratio) + self.BG_GRADIENT_BOTTOM[0] * ratio)
            g = int(self.BG_GRADIENT_TOP[1] * (1 - ratio) + self.BG_GRADIENT_BOTTOM[1] * ratio)
            b = int(self.BG_GRADIENT_TOP[2] * (1 - ratio) + self.BG_GRADIENT_BOTTOM[2] * ratio)
            draw.line([(0, y), (width, y)], fill=(r, g, b))

        # Load fonts
        font_size = 13 * self.SCALE
        title_font_size = 22 * self.SCALE
        legend_font_size = 11 * self.SCALE

        try:
            font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", font_size)
            font_bold = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", font_size)
            title_font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", title_font_size)
            legend_font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", legend_font_size)
        except (OSError, IOError):
            try:
                font = ImageFont.truetype("/usr/share/fonts/TTF/DejaVuSans.ttf", font_size)
                font_bold = ImageFont.truetype("/usr/share/fonts/TTF/DejaVuSans-Bold.ttf", font_size)
                title_font = ImageFont.truetype("/usr/share/fonts/TTF/DejaVuSans-Bold.ttf", title_font_size)
                legend_font = ImageFont.truetype("/usr/share/fonts/TTF/DejaVuSans.ttf", legend_font_size)
            except (OSError, IOError):
                font = ImageFont.load_default()
                font_bold = font
                title_font = font
                legend_font = font

        # Draw title with subtle glow effect
        central_name = family_data['nodes'][user_id]['name']
        title = f"{central_name}'s Family Tree"
        title_bbox = draw.textbbox((0, 0), title, font=title_font)
        title_width = title_bbox[2] - title_bbox[0]
        title_x = (width - title_width) // 2
        title_y = 20 * self.SCALE

        # Title glow
        for offset in range(3, 0, -1):
            alpha = 60 - offset * 15
            glow_color = (255, 255, 255)
            draw.text((title_x, title_y), title, fill=(*glow_color, alpha) if len(glow_color) == 3 else glow_color, font=title_font)
        draw.text((title_x, title_y), title, fill=(255, 255, 255), font=title_font)

        # Precompute node pixel positions
        node_positions = {}
        for uid in positions:
            x = positions[uid][0] * h_spacing + offset_x
            y = positions[uid][1] * v_spacing + offset_y
            node_positions[uid] = (x, y)

        # Group edges by type and sort for better rendering
        marriage_edges = []
        parent_child_edges = []
        for edge in family_data['edges']:
            uid1, uid2, edge_type = edge
            if uid1 in node_positions and uid2 in node_positions:
                if edge_type == 'marriage':
                    marriage_edges.append(edge)
                else:
                    parent_child_edges.append(edge)

        # Draw parent-child edges with smart routing
        self._draw_parent_child_edges(
            draw, parent_child_edges, node_positions,
            node_width, node_height, v_spacing, family_data
        )

        # Draw marriage edges
        self._draw_marriage_edges(draw, marriage_edges, node_positions, node_height)

        # Draw nodes with modern styling
        for uid, node_data in family_data['nodes'].items():
            if uid not in node_positions:
                continue

            x, y = node_positions[uid]
            colors = self.COLORS.get(node_data['type'], self.COLORS['extended'])

            self._draw_modern_node(
                draw, x, y, node_width, node_height,
                colors, node_data['name'], font_bold,
                is_self=(node_data['type'] == 'self')
            )

        # Draw modern legend at bottom
        self._draw_legend(draw, width, height, legend_font, self.SCALE)

        # Scale down with high-quality resampling
        final_img = img.resize((base_width, base_height // self.SCALE * self.SCALE // self.SCALE), Image.Resampling.LANCZOS)

        # Actually just resize properly
        final_img = img.resize((width // self.SCALE, height // self.SCALE), Image.Resampling.LANCZOS)

        # Save to BytesIO
        buffer = BytesIO()
        final_img.save(buffer, format='PNG', optimize=True)
        buffer.seek(0)

        return buffer

    def _draw_modern_node(self, draw, x, y, w, h, colors, name, font, is_self=False):
        """Draw a modern styled node with gradient and shadow."""
        main_color, dark_color = colors
        half_w, half_h = w // 2, h // 2
        radius = min(h // 3, half_h - 2)  # Ensure radius doesn't exceed half height

        # Shadow
        shadow_offset = 4
        self._draw_rounded_rect_filled(
            draw,
            x - half_w + shadow_offset, y - half_h + shadow_offset,
            x + half_w + shadow_offset, y + half_h + shadow_offset,
            radius, (20, 20, 20)
        )

        # Main node body
        self._draw_rounded_rect_filled(
            draw,
            x - half_w, y - half_h,
            x + half_w, y + half_h,
            radius, main_color
        )

        # Bottom darker section for depth (only if there's enough space)
        bottom_section_top = y + 2
        bottom_section_bottom = y + half_h
        if bottom_section_bottom > bottom_section_top + radius:
            # Draw darker bottom portion
            draw.rectangle(
                [x - half_w + radius, bottom_section_top,
                 x + half_w - radius, bottom_section_bottom],
                fill=dark_color
            )
            # Bottom corners
            if bottom_section_bottom - radius * 2 >= bottom_section_top:
                draw.ellipse([x - half_w, bottom_section_bottom - radius * 2,
                             x - half_w + radius * 2, bottom_section_bottom], fill=dark_color)
                draw.ellipse([x + half_w - radius * 2, bottom_section_bottom - radius * 2,
                             x + half_w, bottom_section_bottom], fill=dark_color)

        # Subtle highlight on top
        highlight_color = tuple(min(255, c + 30) for c in main_color)
        draw.line(
            [(x - half_w + radius, y - half_h + 2), (x + half_w - radius, y - half_h + 2)],
            fill=highlight_color, width=2
        )

        # Border
        border_color = (255, 255, 255) if is_self else tuple(min(255, c + 50) for c in main_color)
        border_width = 3 if is_self else 2
        self._draw_rounded_rect_outline(
            draw,
            x - half_w, y - half_h,
            x + half_w, y + half_h,
            radius, border_color, border_width
        )

        # Special glow for "self" node
        if is_self:
            for i in range(2):
                self._draw_rounded_rect_outline(
                    draw,
                    x - half_w - i - 1, y - half_h - i - 1,
                    x + half_w + i + 1, y + half_h + i + 1,
                    radius + i, (255, 215, 0), 1
                )

        # Draw name
        display_name = name
        if len(display_name) > 14:
            display_name = display_name[:12] + ".."

        text_bbox = draw.textbbox((0, 0), display_name, font=font)
        text_width = text_bbox[2] - text_bbox[0]
        text_height = text_bbox[3] - text_bbox[1]

        # Calculate brightness for text color
        avg_color = ((main_color[0] + dark_color[0]) // 2,
                     (main_color[1] + dark_color[1]) // 2,
                     (main_color[2] + dark_color[2]) // 2)
        brightness = (avg_color[0] * 299 + avg_color[1] * 587 + avg_color[2] * 114) / 1000
        text_color = (30, 30, 30) if brightness > 140 else (255, 255, 255)

        # Text shadow for readability
        draw.text(
            (x - text_width // 2 + 1, y - text_height // 2 + 1),
            display_name,
            fill=(0, 0, 0) if brightness > 140 else (50, 50, 50),
            font=font
        )
        draw.text(
            (x - text_width // 2, y - text_height // 2),
            display_name,
            fill=text_color,
            font=font
        )

    def _draw_rounded_rect_filled(self, draw, x1, y1, x2, y2, radius, fill):
        """Draw a filled rounded rectangle."""
        # Ensure coordinates are in correct order
        x1, x2 = min(x1, x2), max(x1, x2)
        y1, y2 = min(y1, y2), max(y1, y2)

        # Ensure radius is valid
        max_radius = min((x2 - x1) / 2, (y2 - y1) / 2)
        radius = max(0, min(radius, max_radius))

        if radius < 1:
            # Just draw a simple rectangle
            draw.rectangle([x1, y1, x2, y2], fill=fill)
            return

        # Main rectangles
        if x2 - radius > x1 + radius:
            draw.rectangle([x1 + radius, y1, x2 - radius, y2], fill=fill)
        if y2 - radius > y1 + radius:
            draw.rectangle([x1, y1 + radius, x2, y2 - radius], fill=fill)

        # Corners
        draw.ellipse([x1, y1, x1 + radius * 2, y1 + radius * 2], fill=fill)
        draw.ellipse([x2 - radius * 2, y1, x2, y1 + radius * 2], fill=fill)
        draw.ellipse([x1, y2 - radius * 2, x1 + radius * 2, y2], fill=fill)
        draw.ellipse([x2 - radius * 2, y2 - radius * 2, x2, y2], fill=fill)

    def _draw_rounded_rect_outline(self, draw, x1, y1, x2, y2, radius, outline, width):
        """Draw a rounded rectangle outline."""
        # Lines
        draw.line([(x1 + radius, y1), (x2 - radius, y1)], fill=outline, width=width)
        draw.line([(x1 + radius, y2), (x2 - radius, y2)], fill=outline, width=width)
        draw.line([(x1, y1 + radius), (x1, y2 - radius)], fill=outline, width=width)
        draw.line([(x2, y1 + radius), (x2, y2 - radius)], fill=outline, width=width)

        # Corner arcs
        draw.arc([x1, y1, x1 + radius * 2, y1 + radius * 2], 180, 270, fill=outline, width=width)
        draw.arc([x2 - radius * 2, y1, x2, y1 + radius * 2], 270, 360, fill=outline, width=width)
        draw.arc([x1, y2 - radius * 2, x1 + radius * 2, y2], 90, 180, fill=outline, width=width)
        draw.arc([x2 - radius * 2, y2 - radius * 2, x2, y2], 0, 90, fill=outline, width=width)

    def _draw_marriage_edges(self, draw, edges, positions, node_height):
        """Draw marriage connections with heart symbol."""
        color = self.EDGE_COLORS['marriage']
        for uid1, uid2, _ in edges:
            x1, y1 = positions[uid1]
            x2, y2 = positions[uid2]

            # Draw curved connection for marriages at same level
            if abs(y1 - y2) < 5:  # Same level
                # Simple line with small hearts/dots
                mid_x = (x1 + x2) / 2

                # Draw line
                draw.line([(x1, y1), (x2, y2)], fill=color, width=3)

                # Draw small heart/circle in middle
                heart_size = 8
                draw.ellipse([
                    mid_x - heart_size, y1 - heart_size,
                    mid_x + heart_size, y1 + heart_size
                ], fill=color, outline=(255, 255, 255), width=1)
            else:
                # Curved line for different levels
                draw.line([(x1, y1), (x2, y2)], fill=color, width=3)

    def _draw_parent_child_edges(self, draw, edges, positions, node_width, node_height, v_spacing, family_data):
        """Draw parent-child connections with smart routing to avoid overlaps."""
        color = self.EDGE_COLORS['parent_child']
        line_width = 2

        # Group edges by parent level to route horizontal lines at different heights
        level_edges = defaultdict(list)
        for uid1, uid2, _ in edges:
            x1, y1 = positions[uid1]
            x2, y2 = positions[uid2]

            # Determine which is parent (higher up = lower y)
            if y1 < y2:
                parent_id, child_id = uid1, uid2
            else:
                parent_id, child_id = uid2, uid1

            parent_level = family_data['levels'].get(parent_id, 0)
            level_edges[parent_level].append((parent_id, child_id))

        # Draw edges level by level
        for level, edge_list in level_edges.items():
            # Group children by their horizontal segment to stagger lines
            horizontal_segments = defaultdict(list)

            for parent_id, child_id in edge_list:
                px, py = positions[parent_id]
                cx, cy = positions[child_id]

                # Calculate the horizontal routing level
                mid_y = (py + cy) / 2
                segment_key = (min(px, cx), max(px, cx), round(mid_y / 10))
                horizontal_segments[segment_key].append((parent_id, child_id, px, py, cx, cy))

            # Stagger overlapping horizontal segments
            segment_offsets = {}
            sorted_segments = sorted(horizontal_segments.keys(), key=lambda s: (s[2], s[0]))

            for i, seg_key in enumerate(sorted_segments):
                # Offset each segment group slightly
                segment_offsets[seg_key] = (i % 3 - 1) * 8

            # Draw the edges
            for seg_key, seg_edges in horizontal_segments.items():
                y_offset = segment_offsets.get(seg_key, 0)

                for parent_id, child_id, px, py, cx, cy in seg_edges:
                    half_node_h = node_height // 2

                    # Start from bottom of parent, end at top of child
                    start_y = py + half_node_h
                    end_y = cy - half_node_h

                    # Calculate mid-point with offset to avoid overlaps
                    mid_y = (start_y + end_y) / 2 + y_offset

                    # Draw smooth path: vertical -> horizontal -> vertical
                    # From parent down
                    draw.line([(px, start_y), (px, mid_y)], fill=color, width=line_width)
                    # Horizontal
                    draw.line([(px, mid_y), (cx, mid_y)], fill=color, width=line_width)
                    # To child
                    draw.line([(cx, mid_y), (cx, end_y)], fill=color, width=line_width)

                    # Draw small circles at connection points
                    dot_size = 4
                    draw.ellipse([px - dot_size, start_y - dot_size, px + dot_size, start_y + dot_size], fill=color)
                    draw.ellipse([cx - dot_size, end_y - dot_size, cx + dot_size, end_y + dot_size], fill=color)

    def _draw_legend(self, draw, width, height, font, scale):
        """Draw a modern legend at the bottom."""
        legend_items = [
            ('You', self.COLORS['self'][0]),
            ('Spouse', self.COLORS['spouse'][0]),
            ('Parent', self.COLORS['parent'][0]),
            ('Child', self.COLORS['child'][0]),
            ('Sibling', self.COLORS['sibling'][0]),
            ('In-law', self.COLORS['in_law'][0]),
        ]

        # Calculate legend dimensions
        item_width = 80 * scale
        total_width = len(legend_items) * item_width
        start_x = (width - total_width) // 2
        legend_y = height - 35 * scale

        # Draw subtle background for legend
        padding = 10 * scale
        draw.rounded_rectangle(
            [start_x - padding, legend_y - padding,
             start_x + total_width + padding, legend_y + 20 * scale + padding],
            radius=8 * scale,
            fill=(45, 47, 52),
            outline=(60, 62, 67),
            width=1
        )

        for i, (label, color) in enumerate(legend_items):
            x = start_x + i * item_width
            circle_size = 7 * scale

            # Draw colored circle with subtle border
            draw.ellipse(
                [x, legend_y, x + circle_size * 2, legend_y + circle_size * 2],
                fill=color,
                outline=(255, 255, 255),
                width=1
            )

            # Draw label
            draw.text(
                (x + circle_size * 2 + 6 * scale, legend_y + 2 * scale),
                label,
                fill=(200, 200, 200),
                font=font
            )

    def _calculate_smart_positions(
        self, family_data: dict, central_user_id: int
    ) -> Dict[int, Tuple[float, float]]:
        """
        Calculate positions with smart grouping:
        - Spouses placed adjacent to each other
        - Children centered under their parents
        - Minimize edge crossings
        """
        nodes = family_data['nodes']
        edges = family_data['edges']
        levels = family_data['levels']

        if not nodes:
            return {}

        positions = {}

        # Build marriage pairs for grouping
        marriage_pairs = {}
        for uid1, uid2, edge_type in edges:
            if edge_type == 'marriage':
                marriage_pairs[uid1] = uid2
                marriage_pairs[uid2] = uid1

        # Build parent-child relationships
        children_of = defaultdict(set)
        parents_of = defaultdict(set)
        for uid1, uid2, edge_type in edges:
            if edge_type == 'parent_child':
                # Determine parent/child by level
                if levels.get(uid1, 0) < levels.get(uid2, 0):
                    children_of[uid1].add(uid2)
                    parents_of[uid2].add(uid1)
                else:
                    children_of[uid2].add(uid1)
                    parents_of[uid1].add(uid2)

        # Group nodes by level
        level_nodes = defaultdict(list)
        for uid, level in levels.items():
            level_nodes[level].append(uid)

        # Sort levels from top (parents) to bottom (children)
        sorted_levels = sorted(level_nodes.keys())

        # Position each level
        for level in sorted_levels:
            nodes_at_level = level_nodes[level]

            if level == 0:
                # Special handling for central level
                # Put central user and spouse first, then siblings
                ordered = []
                added = set()

                # Central user first
                if central_user_id in nodes_at_level:
                    ordered.append(central_user_id)
                    added.add(central_user_id)

                    # Then their spouse(s)
                    if central_user_id in marriage_pairs:
                        spouse = marriage_pairs[central_user_id]
                        if spouse in nodes_at_level and spouse not in added:
                            ordered.append(spouse)
                            added.add(spouse)

                # Then siblings and their spouses
                for uid in nodes_at_level:
                    if uid not in added:
                        ordered.append(uid)
                        added.add(uid)
                        # Add their spouse next to them
                        if uid in marriage_pairs:
                            spouse = marriage_pairs[uid]
                            if spouse in nodes_at_level and spouse not in added:
                                ordered.append(spouse)
                                added.add(spouse)

                nodes_at_level = ordered

            else:
                # For other levels, try to position children under their parents
                # and keep spouses together
                ordered = []
                added = set()

                # Sort by parent position to minimize crossings
                def get_parent_x(uid):
                    parent_positions = [positions.get(p, (0, 0))[0] for p in parents_of.get(uid, [])]
                    if parent_positions:
                        return sum(parent_positions) / len(parent_positions)
                    return 0

                nodes_at_level_sorted = sorted(nodes_at_level, key=get_parent_x)

                for uid in nodes_at_level_sorted:
                    if uid not in added:
                        ordered.append(uid)
                        added.add(uid)
                        # Add spouse next
                        if uid in marriage_pairs:
                            spouse = marriage_pairs[uid]
                            if spouse in nodes_at_level and spouse not in added:
                                ordered.append(spouse)
                                added.add(spouse)

                nodes_at_level = ordered

            # Center nodes at this level
            n = len(nodes_at_level)
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

        This traverses through marriages to show in-laws (children's spouses and their parents).
        """
        nodes: Dict[int, dict] = {}
        edges: List[Tuple[int, int, str]] = []
        levels: Dict[int, int] = {}
        processed_ancestors: Set[int] = set()
        processed_descendants: Set[int] = set()

        async def get_name(uid: int) -> str:
            user = bot.get_user(uid)
            return user.display_name if user else f"User {uid}"

        def add_edge(uid1: int, uid2: int, edge_type: str):
            """Add edge if not already present."""
            edge = tuple(sorted([uid1, uid2]))
            if not any(e[0] == edge[0] and e[1] == edge[1] for e in edges):
                edges.append((uid1, uid2, edge_type))

        async def collect_ancestors(uid: int, current_level: int, max_level: int, node_type: str = 'parent'):
            """Collect parents and grandparents (going up)."""
            if uid in processed_ancestors or current_level < -max_level:
                return
            processed_ancestors.add(uid)

            parents = await db.get_parents(uid)
            for parent_id in parents:
                parent_type = node_type
                if current_level - 1 < -1:
                    parent_type = 'grandparent' if node_type == 'parent' else 'in_law'

                if parent_id not in nodes:
                    nodes[parent_id] = {
                        'name': await get_name(parent_id),
                        'type': parent_type
                    }
                    levels[parent_id] = current_level - 1

                add_edge(parent_id, uid, 'parent_child')
                await collect_ancestors(parent_id, current_level - 1, max_level, node_type)

        async def collect_descendants(uid: int, current_level: int, max_level: int,
                                     is_blood_relative: bool = True, collect_in_laws: bool = True):
            """Collect children, grandchildren, and their spouses (in-laws)."""
            if uid in processed_descendants or current_level > max_level:
                return
            processed_descendants.add(uid)

            children = await db.get_children(uid)
            for child_id in children:
                child_type = 'child' if is_blood_relative else 'in_law'
                if current_level + 1 > 1:
                    child_type = 'grandchild' if is_blood_relative else 'in_law'

                if child_id not in nodes:
                    nodes[child_id] = {
                        'name': await get_name(child_id),
                        'type': child_type
                    }
                    levels[child_id] = current_level + 1

                add_edge(uid, child_id, 'parent_child')

                # Get child's spouses (children-in-law)
                if collect_in_laws:
                    child_spouses = await db.get_spouses(child_id)
                    for spouse_id in child_spouses:
                        if spouse_id not in nodes:
                            nodes[spouse_id] = {
                                'name': await get_name(spouse_id),
                                'type': 'in_law'
                            }
                            levels[spouse_id] = current_level + 1
                        add_edge(child_id, spouse_id, 'marriage')

                        # Get the in-law's parents
                        await collect_ancestors(spouse_id, current_level + 1, max_level, 'in_law')

                        # Get descendants of child's spouse
                        await collect_descendants(spouse_id, current_level + 1, max_level,
                                                 is_blood_relative=False, collect_in_laws=False)

                await collect_descendants(child_id, current_level + 1, max_level,
                                         is_blood_relative=is_blood_relative, collect_in_laws=collect_in_laws)

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
            add_edge(user_id, spouse_id, 'marriage')

            # Get spouse's parents (parents-in-law)
            await collect_ancestors(spouse_id, 0, depth, 'in_law')

        # Add siblings at same level
        siblings = await db.get_siblings(user_id)
        for sibling_id in siblings:
            if sibling_id not in nodes:
                nodes[sibling_id] = {
                    'name': await get_name(sibling_id),
                    'type': 'sibling'
                }
                levels[sibling_id] = 0

            # Get sibling's spouses
            sibling_spouses = await db.get_spouses(sibling_id)
            for spouse_id in sibling_spouses:
                if spouse_id not in nodes:
                    nodes[spouse_id] = {
                        'name': await get_name(spouse_id),
                        'type': 'in_law'
                    }
                    levels[spouse_id] = 0
                add_edge(sibling_id, spouse_id, 'marriage')

        # Collect ancestors (parents, grandparents)
        await collect_ancestors(user_id, 0, depth, 'parent')

        # Collect descendants (children, grandchildren) - this now includes in-laws
        await collect_descendants(user_id, 0, depth, is_blood_relative=True, collect_in_laws=True)

        # Also collect descendants of spouses
        for spouse_id in spouses:
            await collect_descendants(spouse_id, 0, depth, is_blood_relative=True, collect_in_laws=True)

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
