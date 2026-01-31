import logging
from io import BytesIO
from typing import Dict, Optional

import aiohttp

try:
    from PIL import Image, ImageDraw, ImageFont
    PILLOW_AVAILABLE = True
except ImportError:
    PILLOW_AVAILABLE = False

log = logging.getLogger("red.DurkCogs.SocialCredit.idcard")

LOGO_URL = "https://cdn.discordapp.com/attachments/1366837407650943068/1467055862236582032/evil.png?ex=697efdbe&is=697dac3e&hm=7f14e590cb88c5ff98d7dbde3531d2ad01fbe5305e74b110509f68ca6820e3cd&"

# Card dimensions
WIDTH = 600
HEIGHT = 340

# Colour palette - propaganda red/gold theme
BG_DARK = (45, 12, 12)
BG_BANNER = (139, 0, 0)
GOLD = (218, 175, 62)
GOLD_DIM = (170, 135, 45)
TEXT_WHITE = (240, 230, 210)
TEXT_GREY = (160, 145, 130)
DIVIDER_COLOR = (100, 30, 30)
BORDER_COLOR = (180, 140, 50)

# Score tier classifications
CLASSIFICATIONS = [
    (2000, "EXEMPLARY CITIZEN"),
    (1500, "MODEL CITIZEN"),
    (1000, "CITIZEN IN GOOD STANDING"),
    (500, "CITIZEN UNDER REVIEW"),
    (0, "PROBATIONARY CITIZEN"),
]
CLASSIFICATION_DEFAULT = "ENEMY OF THE STATE"


def _get_classification(score: int) -> str:
    for threshold, label in CLASSIFICATIONS:
        if score >= threshold:
            return label
    return CLASSIFICATION_DEFAULT


def _load_fonts() -> Dict[str, "ImageFont.FreeTypeFont"]:
    """Load fonts with platform fallback chain."""
    paths = [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans{}.ttf",
        "/usr/share/fonts/TTF/DejaVuSans{}.ttf",
    ]
    for pattern in paths:
        try:
            return {
                "title": ImageFont.truetype(pattern.format("-Bold"), 18),
                "heading": ImageFont.truetype(pattern.format("-Bold"), 22),
                "body": ImageFont.truetype(pattern.format(""), 16),
                "body_bold": ImageFont.truetype(pattern.format("-Bold"), 16),
                "small": ImageFont.truetype(pattern.format(""), 12),
                "score": ImageFont.truetype(pattern.format("-Bold"), 36),
            }
        except (OSError, IOError):
            continue

    default = ImageFont.load_default()
    return {k: default for k in ("title", "heading", "body", "body_bold", "small", "score")}


async def _fetch_image(url: str, size: tuple[int, int]) -> Optional["Image.Image"]:
    """Download an image from *url* and resize it to *size* ``(w, h)``."""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status != 200:
                    return None
                data = await resp.read()

        img = Image.open(BytesIO(data)).convert("RGBA")
        img = img.resize(size, Image.Resampling.LANCZOS)
        return img
    except Exception as e:
        log.debug(f"Failed to fetch image from {url}: {e}")
        return None


def _circle_crop(img: "Image.Image") -> "Image.Image":
    """Return a copy of *img* cropped to a circle."""
    size = img.width
    mask = Image.new("L", (size, size), 0)
    ImageDraw.Draw(mask).ellipse([0, 0, size - 1, size - 1], fill=255)
    output = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    output.paste(img, mask=mask)
    return output


async def generate_id_card(
    display_name: str,
    user_id: int,
    avatar_url: str,
    score: int,
    rank: int,
    hugs_given: int,
    hugs_received: int,
    pills_taken: int,
    member_since: Optional[str],
) -> BytesIO:
    """Generate a propaganda-style social credit ID card image.

    Returns a BytesIO containing a PNG.
    """
    fonts = _load_fonts()
    img = Image.new("RGBA", (WIDTH, HEIGHT), BG_DARK)
    draw = ImageDraw.Draw(img)

    # ── Watermark logo (behind everything else) ────────────────────
    logo_img = await _fetch_image(LOGO_URL, (220, 220))
    if logo_img:
        # Make it semi-transparent
        logo_alpha = logo_img.split()[3].point(lambda a: int(a * 0.08))
        logo_img.putalpha(logo_alpha)
        # Position: slightly right of centre, vertically centred
        logo_x = WIDTH - 220 - 40
        logo_y = (HEIGHT - 220) // 2
        img.paste(logo_img, (logo_x, logo_y), logo_img)

    draw = ImageDraw.Draw(img)  # refresh draw after paste

    # ── Gold border ──────────────────────────────────────────────────
    draw.rectangle([0, 0, WIDTH - 1, HEIGHT - 1], outline=BORDER_COLOR, width=3)
    draw.rectangle([4, 4, WIDTH - 5, HEIGHT - 5], outline=GOLD_DIM, width=1)

    # ── Top banner ───────────────────────────────────────────────────
    banner_h = 42
    draw.rectangle([3, 3, WIDTH - 4, banner_h], fill=BG_BANNER)

    title_text = "\u2605  SOCIAL CREDIT IDENTIFICATION CARD  \u2605"
    bbox = draw.textbbox((0, 0), title_text, font=fonts["title"])
    tw = bbox[2] - bbox[0]
    draw.text(((WIDTH - tw) / 2, 11), title_text, fill=GOLD, font=fonts["title"])

    # ── Avatar ───────────────────────────────────────────────────────
    avatar_size = 96
    avatar_x, avatar_y = 28, 60
    border_w = 3

    # Draw circular border behind avatar
    draw.ellipse(
        [
            avatar_x - border_w - 1,
            avatar_y - border_w - 1,
            avatar_x + avatar_size + border_w,
            avatar_y + avatar_size + border_w,
        ],
        outline=GOLD,
        width=border_w,
    )

    avatar_img = await _fetch_image(avatar_url, (avatar_size, avatar_size))
    if avatar_img:
        avatar_img = _circle_crop(avatar_img)
        img.paste(avatar_img, (avatar_x, avatar_y), avatar_img)

    # ── Identity fields (right of avatar) ────────────────────────────
    info_x = avatar_x + avatar_size + 24
    info_y = 58

    draw.text((info_x, info_y), "CITIZEN", fill=GOLD_DIM, font=fonts["small"])
    draw.text((info_x, info_y + 14), display_name, fill=TEXT_WHITE, font=fonts["heading"])

    # Score - big and prominent
    score_y = info_y + 46
    draw.text((info_x, score_y), "SCORE", fill=GOLD_DIM, font=fonts["small"])
    draw.text((info_x, score_y + 14), str(score), fill=GOLD, font=fonts["score"])

    # Rank - next to score
    score_text_bbox = draw.textbbox((info_x, score_y + 14), str(score), font=fonts["score"])
    rank_x = score_text_bbox[2] + 16
    draw.text((rank_x, score_y), "RANK", fill=GOLD_DIM, font=fonts["small"])
    draw.text((rank_x, score_y + 16), f"#{rank}", fill=TEXT_WHITE, font=fonts["heading"])

    # ── Divider ──────────────────────────────────────────────────────
    div_y = 172
    draw.line([(20, div_y), (WIDTH - 20, div_y)], fill=DIVIDER_COLOR, width=1)
    record_text = " RECORD "
    rec_bbox = draw.textbbox((0, 0), record_text, font=fonts["small"])
    rec_w = rec_bbox[2] - rec_bbox[0]
    rec_x = (WIDTH - rec_w) / 2
    # Draw background behind label to "break" the line
    draw.rectangle([rec_x - 6, div_y - 7, rec_x + rec_w + 6, div_y + 7], fill=BG_DARK)
    draw.text((rec_x, div_y - 6), record_text, fill=GOLD_DIM, font=fonts["small"])

    # ── Stats section ────────────────────────────────────────────────
    stats_y = div_y + 18
    col1_x = 32
    col2_x = WIDTH / 2 + 16

    def _stat_row(x: int, y: int, label: str, value: str):
        draw.text((x, y), label, fill=TEXT_GREY, font=fonts["body"])
        lbl_bbox = draw.textbbox((x, y), label, font=fonts["body"])
        draw.text((lbl_bbox[2] + 6, y), value, fill=TEXT_WHITE, font=fonts["body_bold"])

    _stat_row(col1_x, stats_y, "Hugs Given:", str(hugs_given))
    _stat_row(col2_x, stats_y, "Hugs Received:", str(hugs_received))
    _stat_row(col1_x, stats_y + 24, "Pills Taken:", str(pills_taken))
    if member_since:
        _stat_row(col2_x, stats_y + 24, "Member Since:", member_since)

    # ── Classification banner ────────────────────────────────────────
    class_y = stats_y + 62
    classification = _get_classification(score)
    draw.line([(20, class_y), (WIDTH - 20, class_y)], fill=DIVIDER_COLOR, width=1)
    class_label = f"CLASSIFICATION: {classification}"
    cls_bbox = draw.textbbox((0, 0), class_label, font=fonts["body_bold"])
    cls_w = cls_bbox[2] - cls_bbox[0]
    draw.text(((WIDTH - cls_w) / 2, class_y + 8), class_label, fill=GOLD, font=fonts["body_bold"])

    # ── User ID at bottom ────────────────────────────────────────────
    id_text = f"ID: {user_id}"
    id_bbox = draw.textbbox((0, 0), id_text, font=fonts["small"])
    id_w = id_bbox[2] - id_bbox[0]
    draw.text(((WIDTH - id_w) / 2, HEIGHT - 22), id_text, fill=TEXT_GREY, font=fonts["small"])

    # ── Save to buffer ───────────────────────────────────────────────
    buf = BytesIO()
    img.save(buf, format="PNG", optimize=True)
    buf.seek(0)
    return buf
