import logging
import math
from io import BytesIO
from typing import TYPE_CHECKING, Dict, List, Set, Optional, Tuple
from collections import defaultdict

import aiohttp

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
        self._crest_cache: Dict[str, Optional[Image.Image]] = {}

    async def _fetch_crest_image(self, url: str, target_size: int = 32) -> Optional[Image.Image]:
        """Fetch and resize a crest image from URL."""
        if not url:
            return None

        # Check cache first
        cache_key = f"{url}_{target_size}"
        if cache_key in self._crest_cache:
            return self._crest_cache[cache_key]

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                    if resp.status != 200:
                        self._crest_cache[cache_key] = None
                        return None
                    data = await resp.read()

            img = Image.open(BytesIO(data))
            # Convert to RGBA if necessary
            if img.mode != 'RGBA':
                img = img.convert('RGBA')

            # Resize to target size maintaining aspect ratio
            img.thumbnail((target_size, target_size), Image.Resampling.LANCZOS)

            # Create a circular mask
            size = (target_size, target_size)
            mask = Image.new('L', size, 0)
            mask_draw = ImageDraw.Draw(mask)
            mask_draw.ellipse([0, 0, target_size - 1, target_size - 1], fill=255)

            # Center the image in the target size
            centered = Image.new('RGBA', size, (0, 0, 0, 0))
            paste_x = (target_size - img.width) // 2
            paste_y = (target_size - img.height) // 2
            centered.paste(img, (paste_x, paste_y))

            # Apply circular mask
            output = Image.new('RGBA', size, (0, 0, 0, 0))
            output.paste(centered, mask=mask)

            self._crest_cache[cache_key] = output
            return output

        except Exception as e:
            log.debug(f"Failed to fetch crest image from {url}: {e}")
            self._crest_cache[cache_key] = None
            return None

    async def _prefetch_crests(self, nodes: Dict, crest_size: int = 32) -> Dict[int, Optional[Image.Image]]:
        """Pre-fetch all crest images for nodes that have them."""
        crests = {}
        for uid, node_data in nodes.items():
            crest_url = node_data.get('crest_url')
            if crest_url:
                crests[uid] = await self._fetch_crest_image(crest_url, crest_size)
            else:
                crests[uid] = None
        return crests

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

        # Create image with gradient background (RGBA for crest transparency)
        img = Image.new('RGBA', (width, height), (*self.BG_COLOR, 255))
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
        self._draw_marriage_edges(draw, marriage_edges, node_positions, node_width, node_height, node_positions)

        # Prefetch crest images (scaled for high-res)
        crest_size = 30 * self.SCALE
        crest_images = await self._prefetch_crests(family_data['nodes'], crest_size)

        # Draw nodes with modern styling
        for uid, node_data in family_data['nodes'].items():
            if uid not in node_positions:
                continue

            x, y = node_positions[uid]
            colors = self.COLORS.get(node_data['type'], self.COLORS['extended'])

            self._draw_modern_node(
                img, draw, x, y, node_width, node_height,
                colors, node_data['name'], font_bold,
                is_self=(node_data['type'] == 'self'),
                crest_img=crest_images.get(uid)
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

    async def generate_server_tree(
        self,
        db: "FamilyDatabase",
        bot: "Red",
    ) -> Optional[BytesIO]:
        """Generate a family tree image for all users with relations."""
        if not PILLOW_AVAILABLE:
            return None

        # Get all users with at least one relation
        all_users = await db.get_all_users_with_relations()

        if not all_users:
            return None

        # Collect all family data
        family_data = await self._collect_all_families(db, bot, all_users)

        if not family_data['nodes']:
            return None

        # Calculate positions for all families
        positions = self._calculate_server_positions(family_data)

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
        base_width = max(800, base_width)
        base_height = max(600, base_height)

        # Cap maximum size to avoid memory issues
        max_dimension = 8000
        if base_width > max_dimension:
            base_width = max_dimension
        if base_height > max_dimension:
            base_height = max_dimension

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

        # Create image with gradient background (RGBA for crest transparency)
        img = Image.new('RGBA', (width, height), (*self.BG_COLOR, 255))
        draw = ImageDraw.Draw(img)

        # Draw subtle gradient background
        for y_pos in range(height):
            ratio = y_pos / height
            r = int(self.BG_GRADIENT_TOP[0] * (1 - ratio) + self.BG_GRADIENT_BOTTOM[0] * ratio)
            g = int(self.BG_GRADIENT_TOP[1] * (1 - ratio) + self.BG_GRADIENT_BOTTOM[1] * ratio)
            b = int(self.BG_GRADIENT_TOP[2] * (1 - ratio) + self.BG_GRADIENT_BOTTOM[2] * ratio)
            draw.line([(0, y_pos), (width, y_pos)], fill=(r, g, b))

        # Load fonts
        font_size = 13 * self.SCALE
        title_font_size = 22 * self.SCALE
        legend_font_size = 11 * self.SCALE

        try:
            font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", font_size)
            font_bold = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", font_size)
            title_font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", title_font_size)
            legend_font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", legend_font_size)
        except (IOError, OSError):
            font = ImageFont.load_default()
            font_bold = font
            title_font = font
            legend_font = font

        # Draw title
        title = "Server Family Tree"
        title_bbox = draw.textbbox((0, 0), title, font=title_font)
        title_width = title_bbox[2] - title_bbox[0]
        title_x = (width - title_width) // 2
        draw.text((title_x, 20 * self.SCALE), title, fill=(220, 220, 230), font=title_font)

        # Calculate actual pixel positions for nodes
        node_positions = {}
        for uid, (grid_x, grid_y) in positions.items():
            px = int(grid_x * h_spacing + offset_x)
            py = int(grid_y * v_spacing + offset_y)
            node_positions[uid] = (px, py)

        # Separate edges by type
        parent_child_edges = [(u1, u2, t) for u1, u2, t in family_data['edges'] if t == 'parent_child']
        marriage_edges = [(u1, u2, t) for u1, u2, t in family_data['edges'] if t == 'marriage']

        # Draw parent-child edges first
        self._draw_parent_child_edges(draw, parent_child_edges, node_positions, node_width, node_height, v_spacing, family_data)

        # Draw marriage edges
        self._draw_marriage_edges(draw, marriage_edges, node_positions, node_width, node_height, node_positions)

        # Prefetch crest images (scaled for high-res)
        crest_size = 30 * self.SCALE
        crest_images = await self._prefetch_crests(family_data['nodes'], crest_size)

        # Draw nodes with modern styling
        for uid, node_data in family_data['nodes'].items():
            if uid not in node_positions:
                continue

            x, y = node_positions[uid]
            colors = self.COLORS.get(node_data['type'], self.COLORS['extended'])

            self._draw_modern_node(
                img, draw, x, y, node_width, node_height,
                colors, node_data['name'], font_bold,
                is_self=False,
                crest_img=crest_images.get(uid)
            )

        # Draw modern legend at bottom
        self._draw_legend(draw, width, height, legend_font, self.SCALE)

        # Scale down with high-quality resampling
        final_img = img.resize((width // self.SCALE, height // self.SCALE), Image.Resampling.LANCZOS)

        # Save to BytesIO
        buffer = BytesIO()
        final_img.save(buffer, format='PNG', optimize=True)
        buffer.seek(0)

        return buffer

    async def _collect_all_families(
        self,
        db: "FamilyDatabase",
        bot: "Red",
        all_users: set
    ) -> Dict:
        """Collect family data for all users, grouping connected families together."""
        nodes = {}
        edges = []
        levels = {}

        async def get_node_info(uid: int) -> dict:
            """Get name and crest URL for a user."""
            user = bot.get_user(uid)
            name = user.display_name if user else f"User {uid}"
            crest_url = None

            # Try to get family profile
            profile = await db.get_family_profile(uid)
            if profile:
                if profile.get("family_title"):
                    title = profile["family_title"]
                    if len(name) + len(title) > 20:
                        name = name[:10] + "..."
                    name = f"{name}\n{title}"
                else:
                    name = name[:15] if len(name) > 15 else name
                crest_url = profile.get("family_crest_url")
            else:
                name = name[:15] if len(name) > 15 else name

            return {"name": name, "crest_url": crest_url}

        def add_edge(uid1: int, uid2: int, edge_type: str):
            edge = (uid1, uid2, edge_type)
            reverse = (uid2, uid1, edge_type)
            if edge not in edges and reverse not in edges:
                edges.append(edge)

        # Process all users
        for user_id in all_users:
            if user_id not in nodes:
                node_info = await get_node_info(user_id)
                nodes[user_id] = {
                    'name': node_info['name'],
                    'crest_url': node_info['crest_url'],
                    'type': 'extended'
                }
                levels[user_id] = 0

            # Get marriages
            spouses = await db.get_spouses(user_id)
            for spouse_id in spouses:
                if spouse_id not in nodes:
                    node_info = await get_node_info(spouse_id)
                    nodes[spouse_id] = {
                        'name': node_info['name'],
                        'crest_url': node_info['crest_url'],
                        'type': 'extended'
                    }
                    levels[spouse_id] = 0
                add_edge(user_id, spouse_id, 'marriage')

            # Get children
            children = await db.get_children(user_id)
            for child_id in children:
                if child_id not in nodes:
                    node_info = await get_node_info(child_id)
                    nodes[child_id] = {
                        'name': node_info['name'],
                        'crest_url': node_info['crest_url'],
                        'type': 'extended'
                    }
                # Child should be at a lower level than parent
                if child_id in levels:
                    if levels[child_id] <= levels[user_id]:
                        levels[child_id] = levels[user_id] + 1
                else:
                    levels[child_id] = levels[user_id] + 1
                add_edge(user_id, child_id, 'parent_child')

            # Get parents
            parents = await db.get_parents(user_id)
            for parent_id in parents:
                if parent_id not in nodes:
                    node_info = await get_node_info(parent_id)
                    nodes[parent_id] = {
                        'name': node_info['name'],
                        'crest_url': node_info['crest_url'],
                        'type': 'extended'
                    }
                # Parent should be at a higher level than child
                if parent_id in levels:
                    if levels[parent_id] >= levels[user_id]:
                        levels[parent_id] = levels[user_id] - 1
                else:
                    levels[parent_id] = levels[user_id] - 1
                add_edge(parent_id, user_id, 'parent_child')

        # Normalize levels so minimum is 0
        if levels:
            min_level = min(levels.values())
            for uid in levels:
                levels[uid] -= min_level

        return {
            'nodes': nodes,
            'edges': edges,
            'levels': levels
        }

    def _calculate_server_positions(self, family_data: Dict) -> Dict[int, Tuple[float, float]]:
        """Calculate positions for all nodes in a server-wide tree."""
        nodes = family_data['nodes']
        edges = family_data['edges']
        levels = family_data['levels']

        if not nodes:
            return {}

        # Find connected components (separate family trees)
        adjacency = defaultdict(set)
        for uid1, uid2, _ in edges:
            adjacency[uid1].add(uid2)
            adjacency[uid2].add(uid1)

        visited = set()
        components = []

        def dfs(start):
            component = set()
            stack = [start]
            while stack:
                node = stack.pop()
                if node in visited:
                    continue
                visited.add(node)
                component.add(node)
                for neighbor in adjacency[node]:
                    if neighbor not in visited:
                        stack.append(neighbor)
            return component

        for uid in nodes:
            if uid not in visited:
                component = dfs(uid)
                if component:
                    components.append(component)

        positions = {}
        x_offset = 0

        # Process each connected component (family tree) separately
        for component in components:
            # Group nodes by level within this component
            level_nodes = defaultdict(list)
            for uid in component:
                level = levels.get(uid, 0)
                level_nodes[level].append(uid)

            # Calculate width of this component
            max_width = max(len(nodes_at_level) for nodes_at_level in level_nodes.values()) if level_nodes else 1

            # Position nodes within component
            for level, nodes_at_level in level_nodes.items():
                num_nodes = len(nodes_at_level)
                start_x = x_offset + (max_width - num_nodes) / 2

                for i, uid in enumerate(sorted(nodes_at_level)):
                    positions[uid] = (start_x + i, level)

            # Move offset for next component
            x_offset += max_width + 2  # Gap between family trees

        return positions

    def _draw_modern_node(self, img, draw, x, y, w, h, colors, name, font, is_self=False, crest_img=None):
        """Draw a modern styled node with gradient and shadow, and optional crest."""
        main_color, dark_color = colors
        half_w, half_h = w // 2, h // 2
        radius = min(h // 3, half_h - 2)  # Ensure radius doesn't exceed half height

        # Shadow (offset down-right)
        shadow_offset = 6
        self._draw_rounded_rect_filled(
            draw,
            x - half_w + shadow_offset, y - half_h + shadow_offset,
            x + half_w + shadow_offset, y + half_h + shadow_offset,
            radius, (15, 15, 18)
        )

        # Main node body with darker color
        self._draw_rounded_rect_filled(
            draw,
            x - half_w, y - half_h,
            x + half_w, y + half_h,
            radius, dark_color
        )

        # Lighter inner area (slightly inset) for gradient effect
        inset = 3
        self._draw_rounded_rect_filled(
            draw,
            x - half_w + inset, y - half_h + inset,
            x + half_w - inset, y + half_h - inset // 2,
            max(1, radius - inset), main_color
        )

        # Subtle highlight line on top
        highlight_color = tuple(min(255, c + 40) for c in main_color)
        draw.line(
            [(x - half_w + radius + 2, y - half_h + 3), (x + half_w - radius - 2, y - half_h + 3)],
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

        # Calculate brightness for text color
        avg_color = ((main_color[0] + dark_color[0]) // 2,
                     (main_color[1] + dark_color[1]) // 2,
                     (main_color[2] + dark_color[2]) // 2)
        brightness = (avg_color[0] * 299 + avg_color[1] * 587 + avg_color[2] * 114) / 1000
        text_color = (30, 30, 30) if brightness > 140 else (255, 255, 255)
        shadow_color = (0, 0, 0) if brightness > 140 else (50, 50, 50)

        # Calculate crest offset if we have a crest
        crest_offset = 0
        if crest_img:
            crest_size = crest_img.width
            crest_offset = crest_size // 2 + 4  # Half crest width + small gap

        # Handle multi-line names (name + title)
        lines = name.split('\n')
        if len(lines) == 1:
            # Single line - original behavior
            display_name = name
            if len(display_name) > 14:
                display_name = display_name[:12] + ".."

            text_bbox = draw.textbbox((0, 0), display_name, font=font)
            text_width = text_bbox[2] - text_bbox[0]
            text_height = text_bbox[3] - text_bbox[1]

            # Adjust x position for crest
            text_center_x = x + crest_offset // 2 if crest_img else x

            # Text shadow for readability
            draw.text(
                (text_center_x - text_width // 2 + 1, y - text_height // 2 + 1),
                display_name,
                fill=shadow_color,
                font=font
            )
            draw.text(
                (text_center_x - text_width // 2, y - text_height // 2),
                display_name,
                fill=text_color,
                font=font
            )
        else:
            # Multi-line (name + title)
            name_line = lines[0][:14] if len(lines[0]) > 14 else lines[0]
            title_line = lines[1][:16] if len(lines[1]) > 16 else lines[1]

            # Get dimensions for both lines
            name_bbox = draw.textbbox((0, 0), name_line, font=font)
            title_bbox = draw.textbbox((0, 0), title_line, font=font)

            name_width = name_bbox[2] - name_bbox[0]
            title_width = title_bbox[2] - title_bbox[0]
            line_height = name_bbox[3] - name_bbox[1]

            # Adjust x position for crest
            text_center_x = x + crest_offset // 2 if crest_img else x

            # Position for two lines centered vertically
            name_y = y - line_height
            title_y = y + 2

            # Draw name line
            draw.text((text_center_x - name_width // 2 + 1, name_y + 1), name_line, fill=shadow_color, font=font)
            draw.text((text_center_x - name_width // 2, name_y), name_line, fill=text_color, font=font)

            # Draw title line (slightly dimmer)
            title_color = tuple(max(0, c - 30) for c in text_color)
            draw.text((text_center_x - title_width // 2 + 1, title_y + 1), title_line, fill=shadow_color, font=font)
            draw.text((text_center_x - title_width // 2, title_y), title_line, fill=title_color, font=font)

        # Draw crest image if available (on the left side of text)
        if crest_img:
            crest_x = int(x - half_w + 6)
            crest_y = int(y - crest_img.height // 2)
            # Paste with alpha mask for transparency
            img.paste(crest_img, (crest_x, crest_y), crest_img)

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

    def _draw_marriage_edges(self, draw, edges, positions, node_width, node_height, all_positions):
        """Draw marriage connections that route around other nodes."""
        color = self.EDGE_COLORS['marriage']
        half_node_w = node_width // 2
        half_node_h = node_height // 2

        for uid1, uid2, _ in edges:
            x1, y1 = positions[uid1]
            x2, y2 = positions[uid2]

            # Ensure x1 < x2 for consistent routing
            if x1 > x2:
                x1, y1, x2, y2 = x2, y2, x1, y1

            # Same level - direct horizontal line between adjacent nodes
            if abs(y1 - y2) < 10:
                # Connect from right edge of left node to left edge of right node
                start_x = x1 + half_node_w
                end_x = x2 - half_node_w
                mid_x = (start_x + end_x) / 2

                # Draw the connecting line
                draw.line([(start_x, y1), (end_x, y2)], fill=color, width=3)

                # Draw small circle in middle
                dot_size = 6
                draw.ellipse([
                    mid_x - dot_size, y1 - dot_size,
                    mid_x + dot_size, y1 + dot_size
                ], fill=color, outline=(255, 255, 255), width=1)
            else:
                # Different levels - route around with elbow connector
                # Go from right of one node, up/down, then to left of other
                start_x = x1 + half_node_w
                end_x = x2 - half_node_w

                # Determine if we go up or down based on relative positions
                if y1 < y2:
                    # uid1 is above uid2
                    route_y = y1 + half_node_h + 15
                else:
                    route_y = y1 - half_node_h - 15

                mid_x = (start_x + end_x) / 2

                # Draw path: horizontal from start, vertical, horizontal to end
                draw.line([(start_x, y1), (mid_x, y1)], fill=color, width=3)
                draw.line([(mid_x, y1), (mid_x, y2)], fill=color, width=3)
                draw.line([(mid_x, y2), (end_x, y2)], fill=color, width=3)

                # Draw dot at midpoint
                dot_size = 5
                mid_y = (y1 + y2) / 2
                draw.ellipse([
                    mid_x - dot_size, mid_y - dot_size,
                    mid_x + dot_size, mid_y + dot_size
                ], fill=color)

    def _draw_parent_child_edges(self, draw, edges, positions, node_width, node_height, v_spacing, family_data):
        """Draw parent-child connections with smart routing to avoid overlaps."""
        color = self.EDGE_COLORS['parent_child']
        line_width = 2

        # Group edges by parent level to route horizontal lines at different heights
        level_edges = defaultdict(list)
        for uid1, uid2, _ in edges:
            # uid1 is always the parent, uid2 is always the child (as stored by add_edge)
            parent_id, child_id = uid1, uid2

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

                    # Determine visual direction based on actual Y positions
                    # Parent should be above (lower Y), child below (higher Y)
                    if py <= cy:
                        # Normal case: parent above child
                        start_y = py + half_node_h  # Bottom of parent
                        end_y = cy - half_node_h    # Top of child
                    else:
                        # Unusual case: child above parent visually, draw from top of parent to bottom of child
                        start_y = py - half_node_h  # Top of parent
                        end_y = cy + half_node_h    # Bottom of child

                    # Calculate mid-point with offset to avoid overlaps
                    mid_y = (start_y + end_y) / 2 + y_offset

                    # Draw smooth path: vertical -> horizontal -> vertical
                    # From parent
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

        async def get_node_info(uid: int) -> dict:
            """Get name and crest URL for a user."""
            user = bot.get_user(uid)
            name = user.display_name if user else f"User {uid}"
            crest_url = None

            # Try to get family profile
            profile = await db.get_family_profile(uid)
            if profile:
                if profile.get("family_title"):
                    title = profile["family_title"]
                    # Truncate if needed to fit in node
                    if len(name) + len(title) > 20:
                        name = name[:10] + "..."
                    name = f"{name}\n{title}"
                else:
                    name = name[:15] if len(name) > 15 else name
                crest_url = profile.get("family_crest_url")
            else:
                name = name[:15] if len(name) > 15 else name

            return {"name": name, "crest_url": crest_url}

        def add_edge(uid1: int, uid2: int, edge_type: str):
            """Add edge if not already present."""
            edge = tuple(sorted([uid1, uid2]))
            if not any(e[0] == edge[0] and e[1] == edge[1] for e in edges):
                edges.append((uid1, uid2, edge_type))

        async def collect_ancestors(uid: int, current_level: int, max_level: int, is_blood: bool = True):
            """Collect parents and grandparents (going up)."""
            if uid in processed_ancestors or current_level < -max_level:
                return
            processed_ancestors.add(uid)

            parents = await db.get_parents(uid)
            for parent_id in parents:
                # Determine the type based on level and blood relation
                if is_blood:
                    if current_level - 1 == -1:
                        parent_type = 'parent'
                    else:
                        parent_type = 'grandparent'
                else:
                    parent_type = 'in_law'

                if parent_id not in nodes:
                    node_info = await get_node_info(parent_id)
                    nodes[parent_id] = {
                        'name': node_info['name'],
                        'crest_url': node_info['crest_url'],
                        'type': parent_type
                    }
                    levels[parent_id] = current_level - 1
                else:
                    # Ensure parent is always at a higher level (lower number) than child
                    if levels[parent_id] >= levels.get(uid, current_level):
                        levels[parent_id] = levels.get(uid, current_level) - 1

                add_edge(parent_id, uid, 'parent_child')
                await collect_ancestors(parent_id, current_level - 1, max_level, is_blood)

        async def collect_descendants(uid: int, current_level: int, max_level: int,
                                     is_blood_relative: bool = True, collect_in_laws: bool = True):
            """Collect children, grandchildren, and their spouses (in-laws)."""
            if uid in processed_descendants or current_level > max_level:
                return
            processed_descendants.add(uid)

            children = await db.get_children(uid)
            for child_id in children:
                # Determine type based on level and blood relation
                if is_blood_relative:
                    if current_level + 1 == 1:
                        child_type = 'child'
                    else:
                        child_type = 'grandchild'
                else:
                    child_type = 'in_law'

                if child_id not in nodes:
                    node_info = await get_node_info(child_id)
                    nodes[child_id] = {
                        'name': node_info['name'],
                        'crest_url': node_info['crest_url'],
                        'type': child_type
                    }
                    levels[child_id] = current_level + 1
                else:
                    # Ensure child is always at a lower level (higher number) than parent
                    if levels[child_id] <= levels.get(uid, current_level):
                        levels[child_id] = levels.get(uid, current_level) + 1

                add_edge(uid, child_id, 'parent_child')

                # Get child's spouses (children-in-law)
                if collect_in_laws:
                    child_spouses = await db.get_spouses(child_id)
                    for spouse_id in child_spouses:
                        # Check if spouse is already in tree (possible with incest)
                        spouse_is_blood = spouse_id in nodes and nodes[spouse_id]['type'] not in ('in_law', 'spouse')

                        if spouse_id not in nodes:
                            node_info = await get_node_info(spouse_id)
                            nodes[spouse_id] = {
                                'name': node_info['name'],
                                'crest_url': node_info['crest_url'],
                                'type': 'in_law'
                            }
                            levels[spouse_id] = current_level + 1
                        add_edge(child_id, spouse_id, 'marriage')

                        # Only traverse spouse's family if they're not already a blood relative
                        if not spouse_is_blood:
                            # Get the in-law's parents (not blood related)
                            await collect_ancestors(spouse_id, current_level + 1, max_level, is_blood=False)

                            # Get descendants of child's spouse (not blood related to central user)
                            await collect_descendants(spouse_id, current_level + 1, max_level,
                                                     is_blood_relative=False, collect_in_laws=False)

                await collect_descendants(child_id, current_level + 1, max_level,
                                         is_blood_relative=is_blood_relative, collect_in_laws=collect_in_laws)

        # Add central user
        node_info = await get_node_info(user_id)
        nodes[user_id] = {
            'name': node_info['name'],
            'crest_url': node_info['crest_url'],
            'type': 'self'
        }
        levels[user_id] = 0

        # Add spouses at same level
        spouses = await db.get_spouses(user_id)
        for spouse_id in spouses:
            node_info = await get_node_info(spouse_id)
            nodes[spouse_id] = {
                'name': node_info['name'],
                'crest_url': node_info['crest_url'],
                'type': 'spouse'
            }
            levels[spouse_id] = 0
            add_edge(user_id, spouse_id, 'marriage')

            # Get spouse's parents (parents-in-law)
            await collect_ancestors(spouse_id, 0, depth, is_blood=False)

        # Add siblings at same level
        siblings = await db.get_siblings(user_id)
        for sibling_id in siblings:
            if sibling_id not in nodes:
                node_info = await get_node_info(sibling_id)
                nodes[sibling_id] = {
                    'name': node_info['name'],
                    'crest_url': node_info['crest_url'],
                    'type': 'sibling'
                }
                levels[sibling_id] = 0

            # Get sibling's spouses
            sibling_spouses = await db.get_spouses(sibling_id)
            for spouse_id in sibling_spouses:
                if spouse_id not in nodes:
                    node_info = await get_node_info(spouse_id)
                    nodes[spouse_id] = {
                        'name': node_info['name'],
                        'crest_url': node_info['crest_url'],
                        'type': 'in_law'
                    }
                    levels[spouse_id] = 0
                add_edge(sibling_id, spouse_id, 'marriage')

        # Collect ancestors (parents, grandparents)
        await collect_ancestors(user_id, 0, depth, is_blood=True)

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
