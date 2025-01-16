#!/usr/bin/env python3

import os
import sys
import time
import json
import logging
import hashlib
import threading
import requests
import shutil
import ffmpeg
import asyncio
import aiohttp
import sqlite3
from datetime import datetime, timedelta
from yt_dlp import YoutubeDL
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn
from rich.prompt import Prompt, Confirm
from rich.panel import Panel
from rich.text import Text
from rich.table import Table
from rich import box
from rich.layout import Layout
from rich.live import Live
from rich.syntax import Syntax
from typing import List, Dict, Optional, Union, Any, Tuple, Callable
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from enum import Enum
from PIL import Image
import ffmpeg 
from ffmpeg import Error as FFmpegError
import aiofiles
from functools import lru_cache
from contextlib import contextmanager, suppress
from typing import Generator, Iterator

class DownloadStatus(Enum):
    """Enhanced download status tracking with more descriptive states"""
    QUEUED = "queued"
    DOWNLOADING = "downloading" 
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    PROCESSING = "processing"
    CONVERTING = "converting"
    VALIDATING = "validating"
    ARCHIVED = "archived"
    SCHEDULED = "scheduled"  
    RETRYING = "retrying"   

@dataclass 
class DownloadInfo:
    """Enhanced download information with additional metadata"""
    url: str
    format_id: str
    filename: str
    status: DownloadStatus
    progress: float
    speed: str
    eta: str
    size: int
    downloaded_size: int
    start_time: datetime
    end_time: Optional[datetime] = None
    error: Optional[str] = None
    thumbnail_path: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    video_quality: Optional[str] = None
    subtitles: Optional[Dict[str, str]] = None
    retry_count: int = 0
    checksum: Optional[str] = None
    schedule_time: Optional[datetime] = None  
    priority: int = 0  
    tags: List[str] = field(default_factory=list)  

class VideoProcessor:
    """Enhanced video processor with better performance and resilience"""
    
    def __init__(self, input_path: str, output_path: str, config: Dict[str, Any]):
        self.input_path = Path(input_path)
        self.output_path = Path(output_path)
        self.config = config
        self._temp_files: List[Path] = []
        self._progress_callback: Optional[Callable[[float], None]] = None
        self._cancel_flag = threading.Event()
        
    def __enter__(self):
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cleanup()
        
    def cleanup(self) -> None:
        """Clean up temporary files safely"""
        for temp_file in self._temp_files:
            try:
                if temp_file.exists():
                    temp_file.unlink()
            except Exception as e:
                logging.warning(f"Failed to cleanup temp file {temp_file}: {str(e)}")
                
    def set_progress_callback(self, callback: Callable[[float], None]) -> None:
        """Set callback for progress updates"""
        self._progress_callback = callback

    def cancel_processing(self) -> None:
        """Cancel ongoing processing"""
        self._cancel_flag.set()

    @contextmanager
    def _processing_context(self) -> Generator[None, None, None]:
        """Context manager for safe processing with cleanup"""
        try:
            self._cancel_flag.clear()
            yield
        finally:
            self.cleanup()
            if self._progress_callback:
                self._progress_callback(100.0)

    async def process_video(self, options: Dict[str, Any]) -> bool:
        """Improved video processing with better error handling and performance"""
        async with self._processing_context():
            try:
                if not await self._validate_input():
                    return False

                # Process tasks concurrently with proper resource management
                tasks = self._create_processing_tasks(options)
                if not tasks:
                    return True  # No tasks to process is considered success

                return await self._execute_tasks(tasks)

            except Exception as e:
                logging.error(f"Video processing error: {str(e)}", exc_info=True)
                return False

    async def _validate_input(self) -> bool:
        """Validate input file and create output directory"""
        try:
            if not self.input_path.exists():
                raise FileNotFoundError(f"Input file not found: {self.input_path}")
            
            # Create output directory with proper permissions
            self.output_path.parent.mkdir(parents=True, exist_ok=True, mode=0o755)
            return True
        except Exception as e:
            logging.error(f"Validation error: {str(e)}")
            return False

    def _create_processing_tasks(self, options: Dict[str, Any]) -> List[asyncio.Task]:
        """Create processing tasks based on options with proper resource limits"""
        tasks = []
        total_tasks = sum(1 for opt in options.values() if opt)
        completed_tasks = 0

        # Map of task types to their processing functions
        task_map = {
            'extract_audio': lambda: self.extract_audio(options.get('audio_format', 'mp3')),
            'compress': lambda: self.compress_video(options.get('target_size')),
            'watermark': lambda: self.add_watermark(options.get('watermark_text')),
            'trim': lambda: self.trim_video(options.get('start_time'), options.get('end_time')),
            'convert_format': lambda: self.convert_format(options.get('target_format'))
        }

        for task_name, task_func in task_map.items():
            if options.get(task_name) and not self._cancel_flag.is_set():
                task = asyncio.create_task(task_func())
                tasks.append(task)
                completed_tasks += 1
                if self._progress_callback:
                    self._progress_callback(completed_tasks / total_tasks * 100)

        return tasks

    async def _execute_tasks(self, tasks: List[asyncio.Task]) -> bool:
        """Execute tasks with proper error handling and resource management"""
        try:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            return all(not isinstance(r, Exception) and r for r in results)
        except Exception as e:
            logging.error(f"Task execution error: {str(e)}")
            return False

    async def _run_async_ffmpeg(self, stream) -> None:
        """Enhanced FFmpeg execution with better error handling and resource management"""
        try:
            process = await asyncio.create_subprocess_exec(
                *stream.get_args(),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                limit=1024*1024  # 1MB buffer limit
            )

            # Use asyncio.wait_for to implement timeout
            try:
                stdout, stderr = await asyncio.wait_for(
                    process.communicate(),
                    timeout=self.config.get('ffmpeg_timeout', 3600)  # 1 hour default timeout
                )
            except asyncio.TimeoutError:
                process.kill()
                raise FFmpegError("FFmpeg process timed out")

            if process.returncode != 0:
                raise FFmpegError(f"FFmpeg error: {stderr.decode()}")

        except Exception as e:
            raise FFmpegError(f"FFmpeg execution error: {str(e)}")

    @contextmanager
    def _temp_file(self, suffix: str = '') -> Iterator[Path]:
        """Safe temporary file management"""
        temp_path = Path(self.config['temp_dir']) / f"temp_{int(time.time())}_{os.urandom(4).hex()}{suffix}"
        try:
            yield temp_path
            self._temp_files.append(temp_path)
        except Exception as e:
            if temp_path.exists():
                temp_path.unlink()
            raise e

    @lru_cache(maxsize=32)
    def _get_audio_codec(self, format: str) -> str:
        """Cached audio codec mapping for better performance"""
        codec_map = {
            'mp3': 'libmp3lame',
            'aac': 'aac',
            'opus': 'libopus',
            'vorbis': 'libvorbis'
        }
        return codec_map.get(format, 'libmp3lame')

    def _get_format_options(self, format: str) -> Dict[str, str]:
        format_presets = {
            'mp4': {
                'vcodec': 'libx264',
                'acodec': 'aac',
                'preset': 'medium',
                'crf': '23'
            },
            'webm': {
                'vcodec': 'libvpx-vp9',
                'acodec': 'libopus',
                'deadline': 'good',
                'cpu-used': '2'
            },
            'mkv': {
                'vcodec': 'libx265',
                'acodec': 'libopus',
                'preset': 'medium',
                'crf': '28'
            }
        }
        return format_presets.get(format, format_presets['mp4'])

    def get_file_hash(self, file_path: str) -> str:
        """Calculate SHA-256 hash of file"""
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()

    def validate_output(self, output_path: str) -> bool:
        """Validate processed video file"""
        try:
            probe = ffmpeg.probe(output_path)
            if not probe:
                return False
            return True
        except FFmpegError:
            return False
        
    async def extract_audio(self, audio_format: str = 'mp3') -> bool:
        """Extract audio with improved error handling and temp file management"""
        with self._temp_file(f'.{audio_format}') as temp_output:
            try:
                stream = ffmpeg.input(self.input_path)
                stream = ffmpeg.output(
                    stream, 
                    str(temp_output),
                    acodec=self._get_audio_codec(audio_format),
                    audio_bitrate=self.config.get('audio_bitrate', '192k'),
                    loglevel='error'
                )
                await self._run_async_ffmpeg(stream)
                shutil.move(str(temp_output), str(self.output_path))
                return True
            except FFmpegError as e:
                logging.error(f"Audio extraction error: {str(e)}")
                return False

    async def compress_video(self, target_size: int) -> bool:
        """Compress video with improved bitrate calculation and temp file handling"""
        with self._temp_file('.mp4') as temp_output:
            try:
                # Get video duration and calculate target bitrate
                probe = await asyncio.to_thread(ffmpeg.probe, self.input_path)
                duration = float(probe['format']['duration'])
                target_bitrate = max(int((target_size * 8) / duration), 128)  # Minimum 128kbps
                
                # Set up compression with quality preservation
                stream = ffmpeg.input(self.input_path)
                stream = ffmpeg.output(
                    stream, 
                    str(temp_output),
                    video_bitrate=f"{target_bitrate}k",
                    audio_bitrate=self.config.get('audio_bitrate', '192k'),
                    preset=self.config.get('compression_preset', 'medium'),
                    crf=self.config.get('crf', 23),
                    maxrate=f"{int(target_bitrate * 1.5)}k",
                    bufsize=f"{target_bitrate * 2}k",
                    loglevel='error'
                )
                await self._run_async_ffmpeg(stream)
                shutil.move(str(temp_output), str(self.output_path))
                return True
            except Exception as e:
                logging.error(f"Compression error: {str(e)}")
                return False

    async def add_watermark(self, text: str) -> bool:
        """Add watermark with improved text rendering and positioning"""
        with self._temp_file('.mp4') as temp_output:
            try:
                # Validate watermark text
                if not text or len(text) > 100:
                    raise ValueError("Invalid watermark text length")

                # Configure watermark with better defaults
                stream = ffmpeg.input(self.input_path)
                stream = ffmpeg.drawtext(
                    stream, 
                    text=text,
                    x=self.config.get('watermark_x', '(w-text_w)/2'),
                    y=self.config.get('watermark_y', '(h-text_h)/2'),
                    fontsize=self.config.get('watermark_fontsize', 24),
                    fontcolor=self.config.get('watermark_color', 'white'),
                    alpha=self.config.get('watermark_alpha', 0.5),
                    font=self.config.get('watermark_font', 'Arial'),
                    shadowcolor=self.config.get('watermark_shadow', 'black'),
                    shadowx=2,
                    shadowy=2
                )
                stream = ffmpeg.output(stream, str(temp_output), loglevel='error')
                await self._run_async_ffmpeg(stream)
                shutil.move(str(temp_output), str(self.output_path))
                return True
            except Exception as e:
                logging.error(f"Watermark error: {str(e)}")
                return False

    async def trim_video(self, start_time: str, end_time: str) -> bool:
        """Trim video with improved timestamp handling and seeking"""
        with self._temp_file('.mp4') as temp_output:
            try:
                # Validate timestamps
                if not self._validate_timestamp(start_time) or not self._validate_timestamp(end_time):
                    raise ValueError("Invalid timestamp format")

                stream = ffmpeg.input(
                    self.input_path,
                    ss=start_time,
                    t=end_time,
                    accurate_seek=True
                )
                stream = ffmpeg.output(
                    stream,
                    str(temp_output),
                    acodec='copy',
                    vcodec='copy',
                    loglevel='error'
                )
                await self._run_async_ffmpeg(stream)
                shutil.move(str(temp_output), str(self.output_path))
                return True
            except Exception as e:
                logging.error(f"Trim error: {str(e)}")
                return False

    async def convert_format(self, target_format: str) -> bool:
        """Convert video format with optimized settings for each format"""
        with self._temp_file(f'.{target_format}') as temp_output:
            try:
                format_options = self._get_format_options(target_format)
                stream = ffmpeg.input(self.input_path)
                stream = ffmpeg.output(
                    stream,
                    str(temp_output),
                    **format_options,
                    loglevel='error'
                )
                await self._run_async_ffmpeg(stream)
                shutil.move(str(temp_output), str(self.output_path))
                return True
            except Exception as e:
                logging.error(f"Format conversion error: {str(e)}")
                return False

    @lru_cache(maxsize=8)
    def _get_format_options(self, format: str) -> Dict[str, str]:
        """Get optimized format options with caching"""
        format_presets = {
            'mp4': {
                'vcodec': 'libx264',
                'acodec': 'aac',
                'preset': self.config.get('compression_preset', 'medium'),
                'crf': str(self.config.get('crf', 23)),
                'movflags': '+faststart'
            },
            'webm': {
                'vcodec': 'libvpx-vp9',
                'acodec': 'libopus',
                'deadline': 'good',
                'cpu-used': '2',
                'row-mt': '1',
                'tile-columns': '2'
            },
            'mkv': {
                'vcodec': 'libx265',
                'acodec': 'libopus',
                'preset': self.config.get('compression_preset', 'medium'),
                'crf': str(self.config.get('crf', 28)),
                'x265-params': 'pools=auto:frame-threads=auto'
            }
        }
        return format_presets.get(format, format_presets['mp4'])

    def _validate_timestamp(self, timestamp: str) -> bool:
        """Validate timestamp format (HH:MM:SS or MM:SS or SS)"""
        try:
            parts = timestamp.split(':')
            if len(parts) > 3:
                return False
            return all(0 <= int(part) < 60 for part in parts)
        except ValueError:
            return False

    def get_file_hash(self, file_path: str) -> str:
        """Calculate SHA-256 hash of file with progress updates"""
        try:
            sha256_hash = hashlib.sha256()
            file_size = os.path.getsize(file_path)
            processed_size = 0
            
            with open(file_path, "rb") as f:
                for byte_block in iter(lambda: f.read(4096), b""):
                    sha256_hash.update(byte_block)
                    processed_size += len(byte_block)
                    if self._progress_callback:
                        self._progress_callback((processed_size / file_size) * 100)
                        
            return sha256_hash.hexdigest()
        except Exception as e:
            logging.error(f"Hash calculation error: {str(e)}")
            return ""

    async def validate_output(self, output_path: str) -> bool:
        """Validate processed video file with improved checks"""
        try:
            if not os.path.exists(output_path):
                return False
                
            # Check file size
            if os.path.getsize(output_path) == 0:
                return False
                
            # Verify file integrity
            probe = await asyncio.to_thread(ffmpeg.probe, output_path)
            if not probe or 'streams' not in probe:
                return False
                
            # Check for video stream
            has_video = any(s['codec_type'] == 'video' for s in probe['streams'])
            if not has_video:
                return False
            
            return True
        except Exception as e:
            logging.error(f"Download validation error: {str(e)}")
            return False

class DatabaseManager:
    """Manages database operations with improved security and error handling"""
    
    def __init__(self, db_path: str):
        self.db_path = Path(db_path)
        self.db_dir = self.db_path.parent
        
        # Secure directory creation
        try:
            self.db_dir.mkdir(mode=0o755, parents=True, exist_ok=True)
        except Exception as e:
            raise Exception(f"Failed to create database directory: {str(e)}")
            
        # Set proper permissions
        try:
            self.db_dir.chmod(0o755)
        except Exception as e:
            raise Exception(f"Failed to set directory permissions: {str(e)}")
            
        # Initialize database
        if not self.db_path.exists():
            try:
                self.db_path.touch(mode=0o644)
            except Exception as e:
                raise Exception(f"Failed to create database file: {str(e)}")
        
        # Verify database connection
        self._verify_connection()
        self.setup_database()

    def _verify_connection(self) -> None:
        """Verify database connection with timeout"""
        try:
            with sqlite3.connect(str(self.db_path), timeout=20) as conn:
                conn.execute("SELECT 1")
        except sqlite3.Error as e:
            raise Exception(f"Failed to connect to database: {str(e)}")

    def setup_database(self):
        def adapt_datetime(dt):
            return dt.isoformat()
        
        def convert_datetime(s):
            return datetime.fromisoformat(s)
            
        sqlite3.register_adapter(datetime, adapt_datetime)
        sqlite3.register_converter("timestamp", convert_datetime)
        
        try:
            with sqlite3.connect(self.db_path, detect_types=sqlite3.PARSE_DECLTYPES, timeout=20) as conn:
                conn.execute("PRAGMA journal_mode=WAL")  # Enable Write-Ahead Logging for better concurrency
                conn.execute("PRAGMA foreign_keys=ON")   # Enable foreign key constraints
                
                conn.executescript("""
                    CREATE TABLE IF NOT EXISTS downloads (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        url TEXT NOT NULL,
                        filename TEXT NOT NULL,
                        status TEXT NOT NULL,
                        start_time TIMESTAMP,
                        end_time TIMESTAMP,
                        file_size INTEGER,
                        file_hash TEXT,
                        metadata TEXT,
                        video_quality TEXT,
                        thumbnail_path TEXT,
                        subtitles TEXT,
                        retry_count INTEGER DEFAULT 0,
                        checksum TEXT,
                        tags TEXT,
                        notes TEXT
                    );

                    CREATE TABLE IF NOT EXISTS favorites (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        url TEXT NOT NULL,
                        title TEXT NOT NULL,
                        added_date TIMESTAMP,
                        tags TEXT,
                        notes TEXT,
                        category TEXT,
                        rating INTEGER
                    );

                    CREATE TABLE IF NOT EXISTS playlists (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        url TEXT NOT NULL,
                        title TEXT NOT NULL,
                        video_count INTEGER,
                        last_updated TIMESTAMP,
                        auto_download BOOLEAN,
                        download_schedule TEXT,
                        quality_preset TEXT
                    );

                    CREATE TABLE IF NOT EXISTS statistics (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        date DATE NOT NULL,
                        downloads_count INTEGER,
                        success_rate REAL,
                        total_size BIGINT,
                        average_speed REAL
                    );
                """)
                conn.commit()
        except sqlite3.Error as e:
            raise Exception(f"Database setup error: {str(e)}")

    def add_download(self, download_info: DownloadInfo) -> int:
        """Add download record to database"""
        try:
            with sqlite3.connect(str(self.db_path), timeout=20) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO downloads (
                        url, filename, status, start_time, file_size, 
                        metadata, video_quality, thumbnail_path
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    download_info.url, 
                    download_info.filename, 
                    download_info.status.value, 
                    download_info.start_time,
                    download_info.size, 
                    json.dumps(download_info.metadata),
                    download_info.video_quality, 
                    download_info.thumbnail_path
                ))
                conn.commit()
                return cursor.lastrowid
        except sqlite3.Error as e:
            logging.error(f"Database error in add_download: {str(e)}")
            return -1

    def update_download_status(self, download_id: int, status: DownloadStatus) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "UPDATE downloads SET status = ? WHERE id = ?",
                (status.value, download_id)
            )

    def get_download_by_url(self, url: str) -> Optional[Dict]:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            result = cursor.execute(
                "SELECT * FROM downloads WHERE url = ?", 
                (url,)
            ).fetchone()
            return dict(zip([col[0] for col in cursor.description], result)) if result else None
        
class CatchStream:
    # Main application class for video downloading and management
    
    def __init__(self):
        # Version info
        self.version = "1.0.1"  # Updated from 1.0.0
        self.author = "DV64"
        self.console = Console()
        
        # Quality presets configuration
        self.quality_presets = {
            "high": {
                'format': 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best',
                'video_quality': '1080p'
            },
            "medium": {
                'format': 'bestvideo[height<=720][ext=mp4]+bestaudio[ext=m4a]/best[height<=720][ext=mp4]/best',
                'video_quality': '720p'
            },
            "low": {
                'format': 'bestvideo[height<=480][ext=mp4]+bestaudio[ext=m4a]/best[height<=480][ext=mp4]/best',
                'video_quality': '480p'
            }
        }
        
        # Download management system
        self.download_queue = asyncio.Queue()
        self.active_downloads = {}
        self.download_semaphore = asyncio.Semaphore(3)  # Max concurrent downloads
        
        # Core initialization
        self.setup_paths()
        self.load_config()
        
        # Setup optimized event loop
        try:
            import uvloop
            uvloop.install()
        except ImportError:
            pass

        # Initialize core components
        self._setup_logging()
        self.db = DatabaseManager(str(Path(self.base_folder) / "catchstream.db"))
        self.initialize_components()
        
        # Initialize managers
        self.stats_manager = StatisticsManager(self.db)
        self.download_scheduler = DownloadScheduler(self)
        
        # Setup caching system
        self._cache = {}
        self._cache_ttl = 300  # Cache lifetime in seconds

    def display_banner(self):
        """Display application banner with enhanced design"""
        banner = f"""[bold cyan]
╔══════════════════════════════════════════════════════════════════════╗
║   ______      __       __   _____ __                                 ║
║  / ____/___ _/ /______/ /_ / ___// /_________  ____ _____ ___       ║
║ / /   / __ `/ __/ ___/ __ \\\\__ \\/ __/ ___/ _ \\/ __ `/ __ `__ \\     ║
║/ /___/ /_/ / /_/ /__/ / / /__/ / /_/ /  /  __/ /_/ / / / / / /     ║
║\\____/\\__,_/\\__/\\___/_/ /_/____/\\__/_/   \\___/\\__,_/_/ /_/ /_/     ║
║                                                                      ║
║  [yellow]Version: {self.version}[/yellow]  •  [green]By: {self.author}[/green]  •  [blue]2024[/blue]        ║
╚══════════════════════════════════════════════════════════════════════╝[/bold cyan]"""
        
        self.console.print(banner)
        
        # Display quick statistics
        if self.active_downloads:
            stats = Table.grid(padding=1)
            stats.add_row(
                f"[cyan]Active: {len(self.active_downloads)}[/cyan]",
                f"[yellow]Queued: {self.download_queue.qsize()}[/yellow]",
                f"[green]Completed: {self._get_completed_count()}[/green]"
            )
            self.console.print(stats)

    async def download_video(self, url: str, format_id: str = "best", 
                            filename: Optional[str] = None, quality: str = "medium") -> bool:
        """Async video download method"""
        try:
            return await self._async_download_video(url, format_id, filename, quality)
        except Exception as e:
            logging.error(f"Download error: {str(e)}")
            self.console.print(f"\n[red]Download error: {str(e)}[/red]")
            return False

    async def _async_download_video(self, url: str, format_id: str = "best", 
                                filename: Optional[str] = None, quality: str = None) -> bool:
        """Async implementation for video downloading"""
        async with self.download_semaphore:
            try:
                # Check cache for video info
                cache_key = f"video_info_{url}"
                info = self._cache.get(cache_key)
                
                if not info:
                    with YoutubeDL({'quiet': True}) as ydl:
                        info = await asyncio.get_running_loop().run_in_executor(
                            None, 
                            lambda: ydl.extract_info(url, download=False)
                        )
                    self._cache[cache_key] = info

                # Setup download configuration
                safe_filename = filename or self._generate_safe_filename(url, info)
                download_info = self._create_download_info(url, format_id, safe_filename, info)
                self.active_downloads[url] = download_info

                # Get quality settings from presets
                quality_preset = self.quality_presets.get(quality, self.quality_presets['medium'])

                # Configure download settings
                ydl_opts = {
                    'format': quality_preset['format'],
                    'outtmpl': os.path.join(self.paths['downloads']['video'], safe_filename),
                    'quiet': True,
                    'no_warnings': True,
                    'progress_hooks': [self._update_progress_hook],
                    'merge_output_format': 'mp4',
                    'writethumbnail': True,
                    'writesubtitles': True,
                    'subtitlesformat': 'srt',
                    'retries': self.config['download_options']['max_retries'],
                    'fragment_retries': self.config['download_options']['fragment_retries'],
                    'ratelimit': self.config['download_options']['ratelimit'],
                    'socket_timeout': self.config['download_options']['socket_timeout'],
                    'concurrent_fragments': 3,
                    'file_access_retries': 3,
                    'extractor_retries': 3,
                    'buffersize': 1024 * 16
                }

                # Execute download
                result = await self._execute_download(url, ydl_opts)
                
                if result == 0:
                    await self._handle_download_complete(download_info)
                    return True
                return False

            except Exception as e:
                logging.error(f"Download error: {str(e)}")
                if url in self.active_downloads:
                    self.active_downloads[url].status = DownloadStatus.FAILED
                    self.active_downloads[url].error = str(e)
                return False

    def _update_progress_hook(self, d):
        """Progress hook for yt-dlp"""
        try:
            url = d.get('info_dict', {}).get('webpage_url', '')
            if url in self.active_downloads:
                download_info = self.active_downloads[url]
                
                if d['status'] == 'downloading':
                    total = d.get('total_bytes') or d.get('total_bytes_estimate', 0)
                    if total > 0:
                        downloaded = d.get('downloaded_bytes', 0)
                        download_info.progress = (downloaded / total) * 100
                        download_info.downloaded_size = downloaded
                        download_info.size = total
                        
                        # Update speed
                        speed = d.get('speed', 0)
                        if speed:
                            download_info.speed = self.format_speed(speed)
                            
                        # Update ETA
                        eta = d.get('eta', 0)
                        if eta:
                            download_info.eta = str(timedelta(seconds=int(eta)))
                            
                elif d['status'] == 'finished':
                    download_info.status = DownloadStatus.COMPLETED
                    download_info.progress = 100.0
                    download_info.end_time = datetime.now()
                    download_info.speed = "0 B/s"
                    download_info.eta = "0:00"
                    
        except Exception as e:
            logging.error(f"Progress hook error: {str(e)}")

    async def validate_download(self, file_path: str) -> bool:
        """Validate downloaded file integrity"""
        try:
            if not os.path.exists(file_path):
                return False
            
            # Check file size
            if os.path.getsize(file_path) == 0:
                return False
            
            # Verify file integrity
            probe = await asyncio.to_thread(ffmpeg.probe, file_path)
            if not probe or 'streams' not in probe:
                return False
            
            # Check for video stream
            has_video = any(s['codec_type'] == 'video' for s in probe['streams'])
            if not has_video:
                return False
            
            return True
        except Exception as e:
            logging.error(f"Download validation error: {str(e)}")
            return False

    def _download_complete_hook(self, d):
        if d['status'] == 'finished':
            url = d.get('url', '')
            if url in self.active_downloads:
                download = self.active_downloads[url]
                download.status = DownloadStatus.COMPLETED
                download.end_time = datetime.now()

    def _download_error_hook(self, d):
        if d['status'] == 'error':
            url = d.get('url', '')
            if url in self.active_downloads:
                download = self.active_downloads[url]
                download.status = DownloadStatus.FAILED
                download.error = str(d.get('error'))

    def _postprocess_hook(self, d):
        if d['status'] == 'started':
            url = d.get('url', '')
            if url in self.active_downloads:
                download = self.active_downloads[url]
                download.status = DownloadStatus.PROCESSING

    async def close(self):
        await self.session.close()
        self.thread_pool.shutdown()
        self.download_scheduler.stop()

    def _setup_logging(self):  
        log_file = os.path.join(
            self.paths['log'], 
            f"catchstream_{datetime.now().strftime('%Y%m%d')}.log"
        )
        logging.basicConfig(
            filename=log_file,
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )

    def load_config(self):
        """Load configuration with default quality settings"""
        config_path = os.path.join(self.base_folder, "config.json")
        default_config = {
            'theme': 'cyan',
            'auto_cleanup': True,
            'max_concurrent_downloads': 3,
            'download_path': str(Path.home() / 'Downloads' / 'CatchStream'),
            'default_quality': {
                'format': 'bestvideo[height<=720][ext=mp4]+bestaudio[ext=m4a]/best[height<=720][ext=mp4]/best',
                'video_quality': '720p'
            },
            'log_retention_days': 30,
            'audio_bitrate': '192k',
            'compression_preset': 'medium',
            'auto_archive': False,
            'notifications': True,
            'thumbnail_quality': 'high',
            'download_options': {
                'max_retries': 3,
                'fragment_retries': 3,
                'ratelimit': 1000000,
                'socket_timeout': 30
            },
            'interface': {
                'show_progress_bar': True,
                'show_eta': True,
                'show_speed': True,
                'show_file_size': True
            }
        }
        
        try:
            os.makedirs(os.path.dirname(config_path), exist_ok=True)
            if os.path.exists(config_path):
                with open(config_path) as f:
                    saved_config = json.load(f)
                    # Merge saved config with default config to ensure all keys exist
                    self.config = {**default_config, **saved_config}
            else:
                self.config = default_config
                with open(config_path, 'w') as f:
                    json.dump(default_config, f, indent=4)
        except Exception as e:
            logging.error(f"Failed to load config: {str(e)}")
            self.config = default_config

    def setup_paths(self):
        """Setup paths in a cross-platform way"""
        # Get the Downloads folder path for any operating system
        if os.name == 'nt':  # Windows
            downloads_dir = os.path.join(os.path.expanduser('~'), 'Downloads')
        else:  # Linux, MacOS, Android
            downloads_dir = os.path.join(os.path.expanduser('~'), 'Downloads')
            if not os.path.exists(downloads_dir):  # Fallback for some Linux/Android systems
                downloads_dir = os.path.join(os.path.expanduser('~'), 'Download')
        
        # Create CatchStream folder inside Downloads
        self.base_folder = os.path.join(downloads_dir, "CatchStream")
        
        # Create separate folders for video and audio
        self.paths = {
            'config': os.path.join(self.base_folder, "config.json"),
            'log': os.path.join(self.base_folder, "logs"),
            'downloads': {
                'video': os.path.join(self.base_folder, "downloads", "video"),
                'audio': os.path.join(self.base_folder, "downloads", "audio")
            },
            'temp': os.path.join(self.base_folder, "temp"),
            'cache': os.path.join(self.base_folder, "cache"),
            'db': os.path.join(self.base_folder, "catchstream.db"),
            'thumbnails': os.path.join(self.base_folder, "thumbnails"),
            'exports': os.path.join(self.base_folder, "exports"),
            'archive': os.path.join(self.base_folder, "archive")
        }
        
        # Create all directories
        for path in self.paths.values():
            if isinstance(path, dict):  # For nested paths like downloads
                for subpath in path.values():
                    os.makedirs(subpath, mode=0o755, exist_ok=True)
                    # Ensure write permissions
                    if not os.access(subpath, os.W_OK):
                        os.chmod(subpath, 0o755)
            else:
                if not path.endswith(('.db', '.json')):
                    os.makedirs(path, mode=0o755, exist_ok=True)
                    # Ensure write permissions
                    if not os.access(path, os.W_OK):
                        os.chmod(path, 0o755)

    async def process_video(self, url: str, options: Dict[str, Any]) -> bool:
        try:
            download_info = self.active_downloads.get(url)
            if not download_info:
                return False

            processor = VideoProcessor(
                os.path.join(self.paths['downloads'], download_info.filename),
                os.path.join(self.paths['temp'], f"processed_{download_info.filename}"),
                self.config
            )

            success = await processor.process_video(options)
            if success and os.path.exists(processor.output_path):
                shutil.move(processor.output_path, 
                          os.path.join(self.paths['downloads'], download_info.filename))
            return success
        except Exception as e:
            logging.error(f"Video processing error: {str(e)}")
            return False

    async def archive_download(self, url: str) -> bool:
        try:
            download_info = self.active_downloads.get(url)
            if not download_info:
                return False

            archive_path = os.path.join(
                self.paths['archive'],
                f"{datetime.now().strftime('%Y%m')}",
                download_info.filename
            )
            os.makedirs(os.path.dirname(archive_path), exist_ok=True)
            
            source_path = os.path.join(self.paths['downloads'], download_info.filename)
            if os.path.exists(source_path):
                shutil.move(source_path, archive_path)
                download_info.status = DownloadStatus.ARCHIVED
                return True
            return False
        except Exception as e:
            logging.error(f"Archive error: {str(e)}")
            return False

    async def handle_download_command(self, cmd: str, download_id: str):
        """Handle download management commands"""
        try:
            # Find download by ID
            target_url = None
            for url, info in self.active_downloads.items():
                if str(hash(url))[:8] == download_id:
                    target_url = url
                    break
                
            if not target_url:
                self.console.print("[red]Download not found[/red]")
                return
            
            download_info = self.active_downloads[target_url]
            
            if cmd == 'p':
                if download_info.status == DownloadStatus.DOWNLOADING:
                    download_info.status = DownloadStatus.PAUSED
                    self.console.print("[yellow]Download paused[/yellow]")
                elif download_info.status == DownloadStatus.PAUSED:
                    download_info.status = DownloadStatus.QUEUED
                    await self.download_video(target_url)  
                    self.console.print("[green]Download resumed[/green]")
                
            elif cmd == 'c':
                download_info.status = DownloadStatus.CANCELLED
                self.console.print("[red]Download cancelled[/red]")
                
            elif cmd == 'r':
                del self.active_downloads[target_url]
                self.console.print("[yellow]Download removed[/yellow]")
                
            elif cmd == 'a':
                if download_info.status == DownloadStatus.COMPLETED:
                    # Move to archive
                    src = os.path.join(self.paths['downloads']['video'], download_info.filename)
                    dst = os.path.join(self.paths['archive'], download_info.filename)
                    shutil.move(src, dst)
                    del self.active_downloads[target_url]
                    self.console.print("[green]Download archived[/green]")
                else:
                    self.console.print("[red]Only completed downloads can be archived[/red]")
                
        except Exception as e:
            logging.error(f"Command error: {str(e)}")
            self.console.print(f"[red]Error: {str(e)}[/red]")

    def handle_batch_download(self) -> None:
        urls = []
        while True:
            url = Prompt.ask("Enter URL (empty to finish)")
            if not url:
                break
            urls.append(url)
        
        if urls:
            quality = Prompt.ask("Select quality", 
                            choices=["low", "medium", "high"],
                            default="medium")
            for url in urls:
                self.event_loop.run_until_complete(
                    self.download_video(url, quality=quality)
                )
                
    def show_advanced_settings(self):
        """Show and modify advanced settings"""
        while True:
            self.console.clear()
            settings_table = Table(
                title="[bold green]Advanced Settings[/bold green]",
                box=box.DOUBLE,
                show_header=True,
                header_style="bold cyan"
            )
            
            settings_table.add_column("Option", style="cyan")
            settings_table.add_column("Current Value", style="yellow")
            settings_table.add_column("Description", style="white")
            
            # Define settings with their descriptions
            settings = [
                ("Theme", self.config['theme'], "Interface color theme"),
                ("Auto Cleanup", str(self.config['auto_cleanup']), "Automatically clean temporary files"),
                ("Max Downloads", str(self.config['max_concurrent_downloads']), "Maximum concurrent downloads"),
                ("Download Path", self.config['download_path'], "Default download location"),
                ("Video Quality", self.config['default_quality']['video_quality'], "Default video quality"),
                ("Audio Bitrate", self.config['audio_bitrate'], "Default audio quality"),
                ("Auto Archive", str(self.config['auto_archive']), "Automatically archive completed downloads"),
                ("Notifications", str(self.config['notifications']), "Show desktop notifications"),
                ("Back", "", "Return to main menu")
            ]
            
            # Add settings to table
            for i, (name, value, desc) in enumerate(settings, 1):
                settings_table.add_row(f"{i}. {name}", value, desc)
            
            self.console.print(settings_table)
            
            choice = Prompt.ask(
                "\n[bold cyan]Select setting to modify[/bold cyan]",
                choices=[str(i) for i in range(1, len(settings) + 1)]
            )
            
            if choice == str(len(settings)):  # Back option
                break
            
            setting_index = int(choice) - 1
            setting_name = settings[setting_index][0]
            
            if setting_name == "Theme":
                themes = ["cyan", "green", "blue", "magenta", "yellow", "red"]
                new_value = Prompt.ask(
                    "Select theme",
                    choices=themes,
                    default=self.config['theme']
                )
                self.config['theme'] = new_value
                
            elif setting_name == "Auto Cleanup":
                new_value = Confirm.ask("Enable auto cleanup?", default=self.config['auto_cleanup'])
                self.config['auto_cleanup'] = new_value
                
            elif setting_name == "Max Downloads":
                new_value = int(Prompt.ask(
                    "Enter maximum concurrent downloads (1-10)",
                    default=str(self.config['max_concurrent_downloads'])
                ))
                if 1 <= new_value <= 10:
                    self.config['max_concurrent_downloads'] = new_value
                
            elif setting_name == "Download Path":
                new_value = Prompt.ask(
                    "Enter download path",
                    default=self.config['download_path']
                )
                if os.path.exists(os.path.expanduser(new_value)):
                    self.config['download_path'] = new_value
                else:
                    self.console.print("[red]Invalid path![/red]")
                    
            elif setting_name == "Video Quality":
                qualities = {
                    "1": "480p",
                    "2": "720p",
                    "3": "1080p"
                }
                self.console.print("\nAvailable Qualities:")
                for k, v in qualities.items():
                    self.console.print(f"{k}. {v}")
                    
                quality_choice = Prompt.ask("Select quality", choices=list(qualities.keys()))
                self.config['default_quality']['video_quality'] = qualities[quality_choice]
                
            elif setting_name == "Audio Bitrate":
                bitrates = ["128k", "192k", "256k", "320k"]
                new_value = Prompt.ask(
                    "Select audio bitrate",
                    choices=bitrates,
                    default=self.config['audio_bitrate']
                )
                self.config['audio_bitrate'] = new_value
                
            elif setting_name == "Auto Archive":
                new_value = Confirm.ask("Enable auto archive?", default=self.config['auto_archive'])
                self.config['auto_archive'] = new_value
                
            elif setting_name == "Notifications":
                new_value = Confirm.ask("Enable notifications?", default=self.config['notifications'])
                self.config['notifications'] = new_value
            
            # Save changes
            try:
                with open(os.path.join(self.base_folder, "config.json"), 'w') as f:
                    json.dump(self.config, f, indent=4)
                self.console.print("[green]Settings saved successfully![/green]")
            except Exception as e:
                self.console.print(f"[red]Failed to save settings: {str(e)}[/red]")
            
            time.sleep(1)  # Show success/error message briefly

    def get_setting_description(self, setting: str) -> str:
        descriptions = {
            'theme': 'Color theme for the user interface',
            'auto_cleanup': 'Automatically clean temporary files on exit',
            'log_retention_days': 'Number of days to keep log files',
            'audio_bitrate': 'Default audio bitrate for conversions',
            'compression_preset': 'Video compression quality preset'
        }
        return descriptions.get(setting, 'No description available')

    def update_setting(self, setting: str, value: str) -> None:
        """Updates a setting and saves to config file"""
        if setting == 'log_retention_days':
            value = int(value)
        elif setting == 'auto_cleanup':
            value = value.lower() == 'true'
            
        self.config[setting] = value
        with open(self.paths['config'], 'w') as f:
            json.dump(self.config, f, indent=4)
            
    async def show_download_manager(self):
        """Display and manage active downloads"""
        layout = Layout()
        layout.split_column(
            Layout(name="header"),
            Layout(name="downloads"),
            Layout(name="controls")
        )

        while True:
            self.console.clear()
            
            header = Table.grid()
            header.add_row(
                Panel(
                    f"CatchStream Download Manager (Active: {len(self.active_downloads)} | Queued: {len(self.download_queue)})",
                    style=self.config['theme']
                )
            )
            layout["header"].update(header)
            
            downloads_table = Table(
                title="Active Downloads",
                box=box.ROUNDED,
                show_header=True,
                header_style=self.config['theme']
            )
            downloads_table.add_column("ID")
            downloads_table.add_column("Filename")
            downloads_table.add_column("Progress")
            downloads_table.add_column("Speed")
            downloads_table.add_column("ETA")
            downloads_table.add_column("Status")
            downloads_table.add_column("Quality")
            
            for url, info in self.active_downloads.items():
                downloads_table.add_row(
                    str(hash(url))[:8],
                    info.filename,
                    f"{info.progress:.1f}%",
                    info.speed,
                    info.eta,
                    info.status.value,
                    info.video_quality or "N/A"
                )
            
            layout["downloads"].update(Panel(downloads_table))
            
            controls = Table.grid()
            controls.add_row(
                "[P]ause/Resume",
                "[C]ancel",
                "[R]emove",
                "[A]rchive",
                "[B]ack to menu"
            )
            layout["controls"].update(Panel(controls))
            
            self.console.print(layout)
            
            cmd = self.console.input("Enter command: ").lower()
            if cmd == 'b':
                break
            elif cmd in ['p', 'c', 'r', 'a']:
                download_id = self.console.input("Enter download ID: ")
                await self.handle_download_command(cmd, download_id)  

    async def main(self):
        """Main application entry point with enhanced design"""
        try:
            while True:
                self.console.clear()
                self.display_banner()
                
                # Enhanced menu design
                menu_table = Table(
                    title="[bold green]Main Menu[/bold green]",
                    box=box.DOUBLE,
                    show_header=True,
                    header_style="bold cyan",
                    border_style="cyan"
                )
                
                menu_table.add_column("#", style="yellow", justify="center", width=4)
                menu_table.add_column("Option", style="green", width=25)
                menu_table.add_column("Description", style="white", width=45)
                
                menu_options = {
                    "1": ("Download Single Video", "Download individual videos with custom quality options"),
                    "2": ("Download Playlist", "Download complete playlists or selected videos"),
                    "3": ("Batch Download", "Download multiple videos simultaneously"),
                    "4": ("Download Manager", "Monitor and control active downloads"),
                    "5": ("Video Processing", "Process downloaded videos - extract audio, compress, etc."),
                    "6": ("Settings", "Configure application settings"),
                    "7": ("Analytics", "View download statistics"),
                    "8": ("Favorites", "Manage your favorite videos"),
                    "9": ("Archive Manager", "Archive and manage completed downloads"),
                    "10": ("Exit", "Save and exit application")
                }
                
                for key, (value, desc) in menu_options.items():
                    menu_table.add_row(key, value, desc)
                
                self.console.print(menu_table)
                
                # Show active downloads if any
                if self.active_downloads:
                    downloads_panel = Panel(
                        self._get_active_downloads_table(),
                        title="[bold yellow]Active Downloads[/bold yellow]",
                        border_style="yellow"
                    )
                    self.console.print("\n", downloads_panel)
                
                choice = Prompt.ask(
                    "\n[bold cyan]Select option[/bold cyan]",
                    choices=list(menu_options.keys())
                )
                
                # Handle menu choices
                if choice == "1":
                    await self.show_single_video_menu()
                elif choice == "2":
                    await self.show_playlist_menu()
                elif choice == "3":
                    await self.show_batch_download_menu()
                elif choice == "4":
                    await self.show_download_manager()
                elif choice == "5":
                    await self.show_video_processing_menu()
                elif choice == "6":
                    self.show_advanced_settings()
                elif choice == "7":
                    self.show_analytics_menu()
                elif choice == "8":
                    self.show_favorites_menu()
                elif choice == "9":
                    self.show_archive_manager()
                elif choice == "10":
                    if self.config['auto_cleanup']:
                        self.cleanup()
                    self.console.print("\n[yellow]Thanks for using CatchStream! Goodbye![/yellow]")
                    break
                
                # Ask to continue unless exiting
                if choice != "10":
                    if not Prompt.ask("\n[cyan]Continue?[/cyan] (Y/n)", default="y").lower().startswith('y'):
                        break

        except KeyboardInterrupt:
            self.console.print("\n[yellow]Program terminated by user. Goodbye![/yellow]")
        except Exception as e:
            logging.error(f"Unexpected error: {str(e)}")
            self.console.print(f"\n[red]Unexpected error: {str(e)}[/red]")
        finally:
            # Cleanup
            if hasattr(self, 'session'):
                await self.session.close()
            if hasattr(self, 'thread_pool'):
                self.thread_pool.shutdown()

    async def show_single_video_menu(self):
        """Display interface for single video download"""
        self.console.clear()
        panel = Panel(
            "[bold cyan]Single Video Download[/bold cyan]\n\n"
            "Download a single video with custom quality settings",
            box=box.DOUBLE,
            border_style="cyan"
        )
        self.console.print(panel)
        
        # Create options table
        options_table = Table(show_header=False, box=None)
        options_table.add_column("Option", style="yellow")
        options_table.add_column("Value", style="cyan")
        
        # Get user input with better formatting
        url = Prompt.ask("\n[cyan]Enter video URL[/cyan]")
        options_table.add_row("URL", url)
        
        format_id = Prompt.ask(
            "[cyan]Enter format ID[/cyan]",
            default="best",
            show_default=True
        )
        options_table.add_row("Format", format_id)
        
        filename = Prompt.ask("[cyan]Enter filename[/cyan] (optional)")
        if filename:
            options_table.add_row("Filename", filename)
        
        # Show quality selection with description
        quality_panel = Panel(
            "Low: 480p\nMedium: 720p\nHigh: 1080p",
            title="Available Qualities",
            border_style="blue"
        )
        self.console.print("\n", quality_panel)
        
        quality = Prompt.ask(
            "[cyan]Select quality[/cyan]",
            choices=["low", "medium", "high"],
            default="medium"
        )
        options_table.add_row("Quality", quality.upper())
        
        # Show summary
        self.console.print("\n[bold green]Download Summary:[/bold green]")
        self.console.print(options_table)
        
        if Confirm.ask("\n[yellow]Start download?[/yellow]"):
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                TimeElapsedColumn(),
            ) as progress:
                task = progress.add_task("Downloading...", total=100)
                success = await self.download_video(url, format_id, filename, quality)
                
            if success:
                self.console.print("\n[bold green]✓ Download completed successfully![/bold green]")
            else:
                self.console.print("\n[bold red]✗ Download failed![/bold red]")

    async def show_playlist_menu(self):
        """Display interface for playlist download"""
        self.console.clear()
        panel = Panel(
            "[bold cyan]Playlist Download[/bold cyan]\n\n"
            "Download complete playlists or select specific videos",
            box=box.DOUBLE,
            border_style="cyan"
        )
        self.console.print(panel)
        
        url = Prompt.ask("[cyan]Enter playlist URL[/cyan]")
        
        # Show playlist info
        try:
            with YoutubeDL() as ydl:
                playlist_info = await asyncio.get_running_loop().run_in_executor(
                    None,
                    lambda: ydl.extract_info(url, download=False)
                )
                
            info_table = Table(show_header=False, box=box.SIMPLE)
            info_table.add_column("Property", style="yellow")
            info_table.add_column("Value", style="white")
            
            info_table.add_row("Title", playlist_info.get('title', 'Unknown'))
            info_table.add_row("Channel", playlist_info.get('uploader', 'Unknown'))
            info_table.add_row("Videos", str(len(playlist_info.get('entries', []))))
            
            self.console.print("\n[bold green]Playlist Information:[/bold green]")
            self.console.print(info_table)
            
            download_all = Confirm.ask("\n[cyan]Download all videos?[/cyan]", default=True)
            
            if not download_all:
                # Show video selection table
                videos_table = Table(
                    title="Available Videos",
                    show_header=True,
                    header_style="bold cyan"
                )
                videos_table.add_column("#", style="yellow")
                videos_table.add_column("Title", style="white")
                videos_table.add_column("Duration", style="cyan")
                
                for i, video in enumerate(playlist_info['entries'], 1):
                    videos_table.add_row(
                        str(i),
                        video.get('title', 'Unknown'),
                        str(timedelta(seconds=video.get('duration', 0)))
                    )
                
                self.console.print(videos_table)
                indices = Prompt.ask(
                    "\n[cyan]Enter video numbers (comma-separated)[/cyan]"
                )
                selected_indices = [int(i.strip()) for i in indices.split(",")]
            
            quality = Prompt.ask(
                "\n[cyan]Select quality[/cyan]",
                choices=["low", "medium", "high"],
                default="medium"
            )
            
            with Progress() as progress:
                task = progress.add_task("Downloading playlist...", total=100)
                await self.process_playlist(url, download_all, quality)
                
        except Exception as e:
            self.console.print(f"\n[bold red]Error: {str(e)}[/bold red]")

    async def show_batch_download_menu(self):
        """Display interface for batch download"""
        self.console.clear()
        panel = Panel(
            "[bold cyan]Batch Download[/bold cyan]\n\n"
            "Download multiple videos simultaneously\n"
            "Enter one URL per line, leave empty to finish",
            box=box.DOUBLE,
            border_style="cyan"
        )
        self.console.print(panel)
        
        urls = []
        url_table = Table(show_header=True, header_style="bold cyan")
        url_table.add_column("#", style="yellow")
        url_table.add_column("URL", style="white")
        
        counter = 1
        while True:
            url = Prompt.ask(f"\n[cyan]Enter URL #{counter}[/cyan] (empty to finish)")
            if not url:
                break
            urls.append(url)
            url_table.add_row(str(counter), url)
            counter += 1
            self.console.clear()
            self.console.print(panel)
            self.console.print("\n[bold green]Added URLs:[/bold green]")
            self.console.print(url_table)
        
        if urls:
            quality = Prompt.ask(
                "[cyan]Select quality for all downloads[/cyan]",
                choices=["low", "medium", "high"],
                default="medium"
            )
            
            with Progress() as progress:
                tasks = {url: progress.add_task(f"Downloading {url[:30]}...", total=100)
                        for url in urls}
                
                for url in urls:
                    await self.download_video(url, quality=quality)

    async def show_video_processing_menu(self):
        while True:
            self.console.clear()
            options = {
                "1": ("Extract Audio", "Convert video to audio format"),
                "2": ("Compress Video", "Reduce video file size while maintaining quality"),
                "3": ("Add Watermark", "Add text overlay to video"),
                "4": ("Trim Video", "Cut video to specific time range"),
                "5": ("Back", "Return to main menu")
            }
            
            menu_table = Table(title="Video Processing", box=box.ROUNDED, show_header=True)
            menu_table.add_column("#", style="cyan", justify="center")
            menu_table.add_column("Option", style="white")
            menu_table.add_column("Description", style="yellow")
            
            for key, value in options.items():
                self.console.print(f"[{self.config['theme']}]{key}[/{self.config['theme']}] {value}")
            
            choice = Prompt.ask("\nSelect option", choices=list(options.keys()))
            if choice == "5":
                break
                
            url = Prompt.ask("Enter video URL to process")
            if url not in self.active_downloads:
                self.console.print("[red]Video not found in downloads[/red]")
                continue
                
            processing_options = {}
            if choice == "1":
                processing_options['extract_audio'] = True
            elif choice == "2":
                target_size = int(Prompt.ask("Enter target size (MB)")) * 1024 * 1024
                processing_options['compress'] = True
                processing_options['target_size'] = target_size
            elif choice == "3":
                text = Prompt.ask("Enter watermark text")
                processing_options['watermark'] = True
                processing_options['watermark_text'] = text
            elif choice == "4":
                start = Prompt.ask("Enter start time (HH:MM:SS)")
                end = Prompt.ask("Enter end time (HH:MM:SS)")
                processing_options['trim'] = True
                processing_options['start_time'] = start
                processing_options['end_time'] = end
                
            await self.process_video(url, processing_options)

    def show_archive_manager(self):
        while True:
            self.console.clear()
            archive_table = Table(title="Archived Downloads")
            archive_table.add_column("ID")
            archive_table.add_column("Filename")
            archive_table.add_column("Date Archived")
            archive_table.add_column("Size")
            
            for root, _, files in os.walk(self.paths['archive']):
                for file in files:
                    file_path = os.path.join(root, file)
                    stat = os.stat(file_path)
                    archive_table.add_row(
                        str(hash(file_path))[:8],
                        file,
                        datetime.fromtimestamp(stat.st_mtime).strftime('%Y-%m-%d %H:%M:%S'),
                        self.format_size(stat.st_size)
                    )
            
            self.console.print(archive_table)
            
            cmd = Prompt.ask("Enter command ([R]estore, [D]elete, [B]ack)", choices=['r', 'd', 'b'])
            if cmd == 'b':
                break
            elif cmd in ['r', 'd']:
                file_id = Prompt.ask("Enter file ID")
                self.handle_archive_command(cmd, file_id)

    def handle_archive_command(self, cmd: str, file_id: str):
        for root, _, files in os.walk(self.paths['archive']):
            for file in files:
                file_path = os.path.join(root, file)
                if str(hash(file_path))[:8] == file_id:
                    if cmd == 'r':
                        shutil.move(file_path, os.path.join(self.paths['downloads'], file))
                        self.console.print(f"[green]Restored {file}[/green]")
                    elif cmd == 'd':
                        os.remove(file_path)
                        self.console.print(f"[yellow]Deleted {file}[/yellow]")
                    return
        self.console.print("[red]File not found[/red]")

    def cleanup(self):
        # Perform comprehensive system cleanup
        try:
            if self.config['auto_cleanup']:
                # Remove temporary files
                for path in [self.paths['temp'], self.paths['cache']]:
                    if os.path.exists(path):
                        with suppress(Exception):
                            shutil.rmtree(path)
                            os.makedirs(path)
                
                # Clean old log files
                log_retention_days = self.config.get('log_retention_days', 30)
                cutoff_date = datetime.now() - timedelta(days=log_retention_days)
                
                # Process logs in batches
                log_files = os.listdir(self.paths['log'])
                for batch in self._batch_iterator(log_files, batch_size=100):
                    for log_file in batch:
                        log_path = os.path.join(self.paths['log'], log_file)
                        if datetime.fromtimestamp(os.path.getctime(log_path)) < cutoff_date:
                            with suppress(Exception):
                                os.remove(log_path)
                
                # Clear system cache
                self._cache.clear()
                
                # Close active connections
                self.db.close()
                
                # Cancel pending tasks
                for task in asyncio.all_tasks(self.event_loop):
                    task.cancel()
                
                logging.info("Cleanup completed successfully")
                
        except Exception as e:
            logging.error(f"Cleanup error: {str(e)}")

    @staticmethod
    def _batch_iterator(items, batch_size=100):
        """Process items in batches for better memory management"""
        for i in range(0, len(items), batch_size):
            yield items[i:i + batch_size]

    def manage_favorites(self):
        # Add inside CatchStream class
        while True:
            favorites_table = Table(title="Favorites")
            favorites_table.add_column("Title")
            favorites_table.add_column("URL")
            favorites_table.add_column("Tags")
            favorites_table.add_column("Rating")
            
            with sqlite3.connect(self.db.db_path) as conn:
                cursor = conn.cursor()
                for row in cursor.execute("SELECT * FROM favorites"):
                    favorites_table.add_row(row[2], row[1], row[4] or "", str(row[6] or ""))
            
            self.console.print(favorites_table)
            cmd = Prompt.ask("Command", choices=["add", "remove", "tag", "rate", "back"])
            if cmd == "back":
                break
            self.handle_favorite_command(cmd)

    async def process_playlist(self, url: str, download_all: bool, quality: str):
        try:
            with YoutubeDL() as ydl:
                playlist_info = await asyncio.get_running_loop().run_in_executor(
                    None,
                    lambda: ydl.extract_info(url, download=False)
                )
                
                videos = playlist_info['entries']
                
                if not download_all:
                    # Show video selection menu
                    for i, video in enumerate(videos, 1):
                        self.console.print(f"{i}. {video['title']}")
                    selection = Prompt.ask("Enter video numbers (comma-separated)")
                    indices = [int(i)-1 for i in selection.split(",")]
                    videos = [videos[i] for i in indices]
                
                for video in videos:
                    await self.download_video(video['webpage_url'], quality=quality)
                    
        except Exception as e:
            logging.error(f"Playlist processing error: {str(e)}")

    def format_size(self, size_bytes: int) -> str:
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size_bytes < 1024:
                return f"{size_bytes:.2f} {unit}"
            size_bytes /= 1024
        return f"{size_bytes:.2f} TB"

    async def initialize_components(self):
        """Initialize core components and handlers"""
        try:
            # Initialize thread pool for concurrent downloads
            max_workers = self.config.get('max_concurrent_downloads', 3)
            self.thread_pool = ThreadPoolExecutor(max_workers=max_workers)
            
            # Initialize session for HTTP requests
            self.session = aiohttp.ClientSession()
            
            # Initialize download hooks
            self.download_hooks = {
                'progress': self._download_progress_hook,
                'complete': self._download_complete_hook,
                'error': self._download_error_hook
            }
            
            # Initialize download lock
            self.download_lock = asyncio.Lock()
            
            # Initialize progress tracking
            self.progress_bars = {}
            
            # Set up default download options from config
            self.default_options = {
                'format': 'bestvideo+bestaudio/best',
                'merge_output_format': 'mp4',
                'writethumbnail': True,
                'writesubtitles': True,
                'subtitlesformat': 'srt',
                'retries': self.config['download_options']['max_retries'],
                'fragment_retries': self.config['download_options']['fragment_retries'],
                'ratelimit': self.config['download_options']['ratelimit'],
                'socket_timeout': self.config['download_options']['socket_timeout']
            }
            
            # Create temp directories if they don't exist
            for path_key in ['temp', 'cache']:
                os.makedirs(self.paths[path_key], exist_ok=True)
            
            # Create download directories
            for subdir in self.paths['downloads'].values():
                os.makedirs(subdir, exist_ok=True)
                
            logging.info("Components initialized successfully")
            
        except Exception as e:
            logging.error(f"Failed to initialize components: {str(e)}")
            raise

    def _download_progress_hook(self, d):
        """Handle download progress updates with enhanced information"""
        try:
            url = d.get('info_dict', {}).get('webpage_url', '')
            if url in self.active_downloads:
                download = self.active_downloads[url]
                
                if d['status'] == 'downloading':
                    total_bytes = d.get('total_bytes') or d.get('total_bytes_estimate', 0)
                    if total_bytes > 0:
                        downloaded = d.get('downloaded_bytes', 0)
                        percent = (downloaded / total_bytes) * 100
                        speed = d.get('speed', 0)
                        speed_str = f"{speed/1024:.1f} KB/s" if speed else "N/A"
                        
                        # Update download info with enhanced metrics
                        download.progress = percent
                        download.speed = speed_str
                        download.eta = d.get('eta', 'N/A')
                        download.downloaded_size = downloaded
                        download.size = total_bytes
                        
                        # Enhanced progress display with more details
                        self.console.print(
                            f"\r[cyan]Progress: {percent:.1f}% | "
                            f"Speed: {speed_str} | "
                            f"ETA: {download.eta} | "
                            f"Size: {self.format_size(downloaded)}/{self.format_size(total_bytes)} | "
                            f"Quality: {download.video_quality}[/cyan]",
                            end=''
                        )
                        
                elif d['status'] == 'finished':
                    download.status = DownloadStatus.COMPLETED
                    download.end_time = datetime.now()
                    download.progress = 100.0
                    duration = download.end_time - download.start_time
                    self.console.print(
                        f"\n[green]Download completed successfully![/green]\n"
                        f"[cyan]Total time: {duration.total_seconds():.1f}s | "
                        f"Average speed: {self.format_speed(download.size/duration.total_seconds())}[/cyan]"
                    )
                    
                elif d['status'] == 'error':
                    download.status = DownloadStatus.FAILED
                    download.error = str(d.get('error', 'Unknown error'))
                    self.console.print(
                        f"\n[red]Download failed: {download.error}[/red]\n"
                        f"[yellow]Attempt {download.retry_count + 1} of {self.config['download_options']['max_retries']}[/yellow]"
                    )
                    
        except Exception as e:
            logging.error(f"Progress hook error: {str(e)}")
            self.console.print(f"\n[red]Progress tracking error: {str(e)}[/red]")

    def _clean_progress_hook(self, d: Dict[str, Any]) -> None:
        try:
            if d['status'] == 'downloading':
                total = d.get('total_bytes') or d.get('total_bytes_estimate', 0)
                if total > 0:
                    downloaded = d.get('downloaded_bytes', 0)
                    percent = (downloaded / total) * 100
                    speed = d.get('speed', 0)
                    
                    width = 40
                    filled = int(width * percent / 100)
                    bar = "█" * filled + "░" * (width - filled)
                    
                    eta = d.get('eta', 0)
                    eta_str = str(timedelta(seconds=eta)) if eta else "N/A"
                    
                    status_line = (
                        f"\r[cyan]{bar}[/cyan] "
                        f"[green]{percent:.1f}%[/green] "
                        f"([yellow]{self.format_size(downloaded)}[/yellow]/"
                        f"[yellow]{self.format_size(total)}[/yellow]) "
                        f"[magenta]{self.format_speed(speed)}[/magenta] "
                        f"[blue]ETA: {eta_str}[/blue]"
                    )
                    
                    if self.config['interface']['show_file_size']:
                        status_line += f" [white]Size: {self.format_size(total)}[/white]"
                    
                    self.console.print(status_line, end='')

        except Exception as e:
            logging.error(f"Progress display error: {str(e)}")

    def format_speed(self, speed: float) -> str:
        if not speed:
            return "N/A"
        for unit in ['B/s', 'KB/s', 'MB/s', 'GB/s']:
            if speed < 1024:
                return f"{speed:.1f} {unit}"
            speed /= 1024
        return f"{speed:.1f} TB/s"

    async def resume_downloads(self) -> None:
        """Resume paused downloads"""
        try:
            with sqlite3.connect(self.db.db_path) as conn:
                cursor = conn.cursor()
                downloads = cursor.execute(
                    "SELECT url, filename FROM downloads WHERE status = ?",
                    (DownloadStatus.PAUSED.value,)
                ).fetchall()
                
                if downloads and self.config['downloads']['auto_resume']:
                    self.console.print("[yellow]Resuming paused downloads...[/yellow]")
                    for url, filename in downloads:
                        await self.download_video(url, filename=filename)
        except Exception as e:
            logging.error(f"Resume error: {str(e)}")

    def _get_active_downloads_table(self) -> Table:
        table = Table(
            show_header=True,
            header_style="bold cyan",
            box=box.ROUNDED,
            title="Active Downloads",
            title_style="bold yellow"
        )
        
        table.add_column("ID", style="cyan", width=8)
        table.add_column("Filename", style="white", width=30)
        table.add_column("Progress", style="green", width=20)
        table.add_column("Speed", style="yellow", width=12)
        table.add_column("ETA", style="magenta", width=10)
        table.add_column("Size", style="blue", width=10)
        table.add_column("Status", style="cyan", width=10)
        
        for url, info in self.active_downloads.items():
            progress_width = 15
            filled = int(info.progress / 100 * progress_width)
            progress_bar = "█" * filled + "░" * (progress_width - filled)
            
            table.add_row(
                str(hash(url))[:8],
                info.filename[:27] + "..." if len(info.filename) > 30 else info.filename,
                f"{progress_bar} {info.progress:.1f}%",
                info.speed,
                str(info.eta),
                self.format_size(info.size),
                info.status.value
            )
        
        return table

    # Add this method to the CatchStream class

    def _generate_safe_filename(self, url: str, info: dict) -> str:
        """Generate a safe filename from video info or URL"""
        try:
            # Try to get title from video info
            title = info.get('title', '')
            if not title:
                # Fallback to URL if no title
                title = url.split('/')[-1]
                if not title:
                    # Generate random filename if URL has no useful part
                    title = f"video_{int(time.time())}"
            
            # Remove invalid characters
            title = "".join(c for c in title if c.isalnum() or c in (' ', '-', '_')).strip()
            
            # Replace spaces with underscores and limit length
            safe_name = title.replace(' ', '_')[:100]
            
            # Add timestamp to ensure uniqueness
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            
            # Add extension
            extension = info.get('ext', 'mp4')
            
            return f"{safe_name}_{timestamp}.{extension}"
            
        except Exception as e:
            logging.error(f"Error generating filename: {str(e)}")
            # Return a fallback filename if something goes wrong
            return f"download_{int(time.time())}.mp4"

    # Add this method to the CatchStream class

    def _create_download_info(self, url: str, format_id: str, filename: str, info: dict) -> DownloadInfo:
        """Create a new DownloadInfo object for tracking download progress"""
        try:
            return DownloadInfo(
                url=url,
                format_id=format_id,
                filename=filename,
                status=DownloadStatus.QUEUED,
                progress=0.0,
                speed="0 B/s",
                eta="unknown",
                size=info.get('filesize', 0) or info.get('filesize_approx', 0),
                downloaded_size=0,
                start_time=datetime.now(),
                metadata={
                    'title': info.get('title', ''),
                    'uploader': info.get('uploader', ''),
                    'duration': info.get('duration', 0),
                    'view_count': info.get('view_count', 0),
                    'like_count': info.get('like_count', 0),
                    'upload_date': info.get('upload_date', ''),
                    'description': info.get('description', '')
                },
                video_quality=info.get('format_note', ''),
                thumbnail_path=info.get('thumbnail', None),
                subtitles=info.get('subtitles', {}),
                tags=info.get('tags', [])
            )
        except Exception as e:
            logging.error(f"Error creating download info: {str(e)}")
            # Return a basic DownloadInfo object if something goes wrong
            return DownloadInfo(
                url=url,
                format_id=format_id,
                filename=filename,
                status=DownloadStatus.QUEUED,
                progress=0.0,
                speed="0 B/s",
                eta="unknown",
                size=0,
                downloaded_size=0,
                start_time=datetime.now()
            )

    # Add these methods to the CatchStream class

    async def _execute_download(self, url: str, ydl_opts: dict) -> int:
        """Execute download with yt-dlp and handle progress"""
        try:
            with YoutubeDL(ydl_opts) as ydl:
                result = await asyncio.get_running_loop().run_in_executor(
                    self.thread_pool,
                    lambda: ydl.download([url])
                )
                return result
                
        except Exception as e:
            logging.error(f"Download execution error: {str(e)}")
            if url in self.active_downloads:
                self.active_downloads[url].status = DownloadStatus.FAILED
                self.active_downloads[url].error = str(e)
            return 1

    async def _download_with_progress(self, url: str, ydl_opts: dict) -> bool:
        """Download with progress tracking"""
        try:
            download_info = self.active_downloads[url]
            download_info.status = DownloadStatus.DOWNLOADING
            
            result = await self._execute_download(url, ydl_opts)
            
            if result == 0:
                download_info.status = DownloadStatus.COMPLETED
                download_info.progress = 100.0
                download_info.end_time = datetime.now()
                return True
            else:
                download_info.status = DownloadStatus.FAILED
                return False
                
        except Exception as e:
            logging.error(f"Download progress error: {str(e)}")
            return False

    def _update_progress(self, download_info: DownloadInfo, progress_data: dict) -> None:
        """Update download progress information"""
        try:
            if progress_data.get('status') == 'downloading':
                total = progress_data.get('total_bytes') or progress_data.get('total_bytes_estimate', 0)
                
                if total > 0:
                    downloaded = progress_data.get('downloaded_bytes', 0)
                    download_info.progress = (downloaded / total) * 100
                    download_info.downloaded_size = downloaded
                    download_info.size = total
                    
                    # Update speed
                    speed = progress_data.get('speed', 0)
                    if speed:
                        download_info.speed = self.format_speed(speed)
                        
                    # Update ETA
                    eta = progress_data.get('eta', 0)
                    if eta:
                        download_info.eta = str(timedelta(seconds=int(eta)))
                        
        except Exception as e:
            logging.error(f"Progress update error: {str(e)}")

    async def _handle_download_complete(self, download_info: DownloadInfo) -> None:
        """Handle download completion"""
        try:
            download_info.status = DownloadStatus.COMPLETED
            download_info.progress = 100.0
            download_info.end_time = datetime.now()
            download_info.speed = "0 B/s"
            download_info.eta = "0:00"
            
            # Calculate checksum
            file_path = os.path.join(self.paths['downloads']['video'], download_info.filename)
            if os.path.exists(file_path):
                download_info.checksum = await asyncio.to_thread(self.get_file_hash, file_path)
                
            # Update database
            await self.db.update_download_status(download_info.url, download_info.status)
            
            # Show notification if enabled
            if self.config['notifications']:
                self.show_notification("Download Complete", f"Downloaded: {download_info.filename}")
                
        except Exception as e:
            logging.error(f"Download completion error: {str(e)}")

    def show_notification(self, title: str, message: str) -> None:
        """Show desktop notification"""
        try:
            if self.config['notifications']:
                # Try to use platform-specific notification
                try:
                    import plyer
                    plyer.notification.notify(
                        title=title,
                        message=message,
                        app_name="CatchStream"
                    )
                except ImportError:
                    # Fallback to console notification
                    self.console.print(f"\n[bold green]{title}:[/bold green] {message}")
        except Exception as e:
            logging.error(f"Notification error: {str(e)}")

class StatisticsManager:
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager

    def show_analytics(self):
        with sqlite3.connect(self.db.db_path) as conn:
            cursor = conn.cursor()
            
            stats_table = Table(title="Download Statistics")
            stats_table.add_column("Metric")
            stats_table.add_column("Value")
            
            stats = [
                ("Total Downloads", "SELECT COUNT(*) FROM downloads"),
                ("Successful Downloads", "SELECT COUNT(*) FROM downloads WHERE status = 'completed'"),
                ("Failed Downloads", "SELECT COUNT(*) FROM downloads WHERE status = 'failed'"),
                ("Average Download Size", "SELECT AVG(file_size) FROM downloads WHERE file_size IS NOT NULL"),
                ("Total Download Size", "SELECT SUM(file_size) FROM downloads WHERE file_size IS NOT NULL"),
                ("Most Popular Format", """
                    SELECT video_quality, COUNT(*) as count 
                    FROM downloads 
                    GROUP BY video_quality 
                    ORDER BY count DESC 
                    LIMIT 1
                """)
            ]
            
            for label, query in stats:
                result = cursor.execute(query).fetchone()
                value = result[0] if result else "N/A"
                if isinstance(value, (int, float)):
                    if "Size" in label:
                        value = self.format_size(value)
                stats_table.add_row(label, str(value))
            
            return stats_table

    def update_statistics(self, download_info: DownloadInfo) -> None:
        with sqlite3.connect(self.db.db_path) as conn:
            today = datetime.now().date()
            conn.execute("""
                INSERT INTO statistics (date, downloads_count, success_rate, total_size, average_speed)
                VALUES (?, 1, ?, ?, ?)
                ON CONFLICT(date) DO UPDATE SET
                    downloads_count = downloads_count + 1,
                    success_rate = (success_rate * downloads_count + ?) / (downloads_count + 1),
                    total_size = total_size + ?,
                    average_speed = (average_speed * downloads_count + ?) / (downloads_count + 1)
            """, (
                today,
                1.0 if download_info.status == DownloadStatus.COMPLETED else 0.0,
                download_info.size,
                self.parse_speed(download_info.speed),
                1.0 if download_info.status == DownloadStatus.COMPLETED else 0.0,
                download_info.size,
                self.parse_speed(download_info.speed)
            ))

    @staticmethod
    def parse_speed(speed_str: str) -> float:
        """Convert speed string to bytes per second"""
        try:
            if speed_str == 'N/A':
                return 0.0
            value, unit = speed_str.split()
            value = float(value)
            unit = unit.upper()
            multiplier = {
                'B/S': 1,
                'KB/S': 1024,
                'MB/S': 1024*1024,
                'GB/S': 1024*1024*1024
            }.get(unit, 1)
            return value * multiplier
        except:
            return 0.0
        
    @staticmethod
    def format_size(size: float) -> str:
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size < 1024:
                return f"{size:.2f} {unit}"
            size /= 1024
        return f"{size:.2f} TB"

class DownloadScheduler:
    def __init__(self, catchstream_instance):
        self.catchstream = catchstream_instance
        self._scheduled_downloads = {}
        self._running = threading.Event()
        self._running.set()
        self._lock = threading.RLock()  # Use RLock for nested acquisitions
        self.scheduler_thread = threading.Thread(target=self._scheduler_loop, daemon=True)
        self.scheduler_thread.start()

    def stop(self):
        self._running.clear()
        with self._lock:
            self._scheduled_downloads.clear()
        self.scheduler_thread.join(timeout=2)

    def schedule_download(self, url: str, schedule_time: datetime, format_id: str = "best", 
                         filename: Optional[str] = None, quality: str = "medium") -> bool:
        try:
            with self._lock:
                self._scheduled_downloads[url] = {
                    'time': schedule_time,
                    'format_id': format_id,
                    'filename': filename,
                    'quality': quality
                }
            logging.info(f"Scheduled download for {url} at {schedule_time}")
            return True
        except Exception as e:
            logging.error(f"Failed to schedule download: {str(e)}")
            return False

    def cancel_scheduled_download(self, url: str) -> bool:
        with self._lock:
            if url in self._scheduled_downloads:
                del self._scheduled_downloads[url]
                logging.info(f"Cancelled scheduled download for {url}")
                return True
        return False

    def get_scheduled_downloads(self) -> List[Dict[str, Any]]:
        with self._lock:
            return [{
                'url': url,
                'schedule_time': info['time'],
                'quality': info['quality']
            } for url, info in self._scheduled_downloads.items()]

    def _scheduler_loop(self):
        while self._running.is_set():  # Changed from self.running to self._running.is_set()
            try:
                now = datetime.now()
                downloads_to_trigger = []
                
                with self._lock:
                    for url, info in list(self._scheduled_downloads.items()):  # Fixed attribute name
                        if now >= info['time']:
                            downloads_to_trigger.append((url, info))
                            del self._scheduled_downloads[url]  # Fixed attribute name
                
                for url, info in downloads_to_trigger:
                    asyncio.run_coroutine_threadsafe(
                        self.catchstream.download_video(
                            url,
                            info['format_id'],
                            info['filename'],
                            info['quality']
                        ),
                        self.catchstream.event_loop
                    )
                    
                time.sleep(60)
            except Exception as e:
                logging.error(f"Scheduler error: {str(e)}")
                time.sleep(60)

    def show_analytics_menu(self):
        """Display download statistics and analytics"""
        self.console.clear()
        
        # Create analytics panel
        panel = Panel(
            "[bold cyan]Download Analytics[/bold cyan]\n\n"
            "View detailed statistics about your downloads",
            box=box.DOUBLE,
            border_style="cyan"
        )
        self.console.print(panel)
        
        try:
            # Get statistics from database
            with sqlite3.connect(self.db.db_path) as conn:
                cursor = conn.cursor()
                
                # Create statistics table
                stats_table = Table(
                    title="Download Statistics",
                    show_header=True,
                    header_style="bold cyan",
                    box=box.SIMPLE
                )
                
                stats_table.add_column("Metric", style="yellow")
                stats_table.add_column("Value", style="green")
                
                # Total downloads
                total = cursor.execute("SELECT COUNT(*) FROM downloads").fetchone()[0]
                stats_table.add_row("Total Downloads", str(total))
                
                # Completed downloads
                completed = cursor.execute(
                    "SELECT COUNT(*) FROM downloads WHERE status = ?",
                    (DownloadStatus.COMPLETED.value,)
                ).fetchone()[0]
                stats_table.add_row("Completed Downloads", str(completed))
                
                # Failed downloads
                failed = cursor.execute(
                    "SELECT COUNT(*) FROM downloads WHERE status = ?",
                    (DownloadStatus.FAILED.value,)
                ).fetchone()[0]
                stats_table.add_row("Failed Downloads", str(failed))
                
                # Total download size
                total_size = cursor.execute(
                    "SELECT SUM(file_size) FROM downloads"
                ).fetchone()[0] or 0
                stats_table.add_row("Total Size", self.format_size(total_size))
                
                # Average download size
                avg_size = total_size / total if total > 0 else 0
                stats_table.add_row("Average Size", self.format_size(avg_size))
                
                # Most recent download
                recent = cursor.execute(
                    "SELECT filename, start_time FROM downloads ORDER BY start_time DESC LIMIT 1"
                ).fetchone()
                if recent:
                    stats_table.add_row("Latest Download", recent[0])
                    stats_table.add_row("Latest Time", recent[1])
                
                self.console.print(stats_table)
                
                # Show quality distribution
                quality_table = Table(
                    title="\nQuality Distribution",
                    show_header=True,
                    header_style="bold cyan"
                )
                quality_table.add_column("Quality", style="yellow")
                quality_table.add_column("Count", style="green")
                quality_table.add_column("Percentage", style="blue")
                
                for quality in ['low', 'medium', 'high']:
                    count = cursor.execute(
                        "SELECT COUNT(*) FROM downloads WHERE video_quality LIKE ?",
                        (f"%{quality}%",)
                    ).fetchone()[0]
                    percentage = (count / total * 100) if total > 0 else 0
                    quality_table.add_row(
                        quality.title(),
                        str(count),
                        f"{percentage:.1f}%"
                    )
                
                self.console.print(quality_table)
                
                # Wait for user input
                Prompt.ask("\nPress Enter to continue")
                
        except Exception as e:
            logging.error(f"Analytics error: {str(e)}")
            self.console.print(f"\n[red]Error displaying analytics: {str(e)}[/red]")

    def show_favorites_menu(self):
        """Display and manage favorite videos"""
        while True:
            self.console.clear()
            
            # Create favorites panel
            panel = Panel(
                "[bold cyan]Favorites Manager[/bold cyan]\n\n"
                "Manage your favorite videos and playlists",
                box=box.DOUBLE,
                border_style="cyan"
            )
            self.console.print(panel)
            
            try:
                # Create favorites table
                favorites_table = Table(
                    show_header=True,
                    header_style="bold cyan",
                    box=box.SIMPLE
                )
                
                favorites_table.add_column("ID", style="yellow", width=8)
                favorites_table.add_column("Title", style="white", width=40)
                favorites_table.add_column("URL", style="blue", width=30)
                favorites_table.add_column("Tags", style="green", width=20)
                favorites_table.add_column("Rating", style="magenta", width=8)
                
                # Get favorites from database
                with sqlite3.connect(self.db.db_path) as conn:
                    cursor = conn.cursor()
                    cursor.execute("""
                        SELECT id, title, url, tags, rating 
                        FROM favorites 
                        ORDER BY rating DESC, title
                    """)
                    
                    for row in cursor.fetchall():
                        favorites_table.add_row(
                            str(row[0]),
                            row[1][:37] + "..." if len(row[1]) > 40 else row[1],
                            row[2][:27] + "..." if len(row[2]) > 30 else row[2],
                            row[3] or "",
                            "★" * int(row[4]) if row[4] else ""
                        )
                
                self.console.print(favorites_table)
                
                # Show options
                self.console.print("\n[bold cyan]Options:[/bold cyan]")
                self.console.print("1. Add favorite")
                self.console.print("2. Remove favorite")
                self.console.print("3. Edit tags")
                self.console.print("4. Rate favorite")
                self.console.print("5. Download favorite")
                self.console.print("6. Back to main menu")
                
                choice = Prompt.ask(
                    "\n[bold cyan]Select option[/bold cyan]",
                    choices=["1", "2", "3", "4", "5", "6"]
                )
                
                if choice == "1":
                    # Add new favorite
                    url = Prompt.ask("[cyan]Enter video URL[/cyan]")
                    title = Prompt.ask("[cyan]Enter title[/cyan] (optional)")
                    tags = Prompt.ask("[cyan]Enter tags[/cyan] (comma-separated, optional)")
                    
                    if not title:
                        # Get title from video info
                        with YoutubeDL({'quiet': True}) as ydl:
                            info = ydl.extract_info(url, download=False)
                            title = info.get('title', 'Unknown Title')
                    
                    with sqlite3.connect(self.db.db_path) as conn:
                        conn.execute("""
                            INSERT INTO favorites (url, title, tags)
                            VALUES (?, ?, ?)
                        """, (url, title, tags))
                        
                    self.console.print("[green]Favorite added successfully![/green]")
                    
                elif choice == "2":
                    # Remove favorite
                    fav_id = Prompt.ask("[cyan]Enter favorite ID to remove[/cyan]")
                    with sqlite3.connect(self.db.db_path) as conn:
                        conn.execute("DELETE FROM favorites WHERE id = ?", (fav_id,))
                    self.console.print("[yellow]Favorite removed![/yellow]")
                    
                elif choice == "3":
                    # Edit tags
                    fav_id = Prompt.ask("[cyan]Enter favorite ID[/cyan]")
                    new_tags = Prompt.ask("[cyan]Enter new tags[/cyan] (comma-separated)")
                    with sqlite3.connect(self.db.db_path) as conn:
                        conn.execute(
                            "UPDATE favorites SET tags = ? WHERE id = ?",
                            (new_tags, fav_id)
                        )
                    self.console.print("[green]Tags updated![/green]")
                    
                elif choice == "4":
                    # Rate favorite
                    fav_id = Prompt.ask("[cyan]Enter favorite ID[/cyan]")
                    rating = int(Prompt.ask(
                        "[cyan]Enter rating[/cyan] (1-5)",
                        choices=["1", "2", "3", "4", "5"]
                    ))
                    with sqlite3.connect(self.db.db_path) as conn:
                        conn.execute(
                            "UPDATE favorites SET rating = ? WHERE id = ?",
                            (rating, fav_id)
                        )
                    self.console.print("[green]Rating updated![/green]")
                    
                elif choice == "5":
                    # Download favorite
                    fav_id = Prompt.ask("[cyan]Enter favorite ID to download[/cyan]")
                    with sqlite3.connect(self.db.db_path) as conn:
                        cursor = conn.cursor()
                        cursor.execute("SELECT url FROM favorites WHERE id = ?", (fav_id,))
                        result = cursor.fetchone()
                        if result:
                            url = result[0]
                            quality = Prompt.ask(
                                "[cyan]Select quality[/cyan]",
                                choices=["low", "medium", "high"],
                                default="medium"
                            )
                            self.event_loop.run_until_complete(
                                self.download_video(url, quality=quality)
                            )
                        else:
                            self.console.print("[red]Favorite not found![/red]")
                    
                elif choice == "6":
                    break
                    
                if choice != "6":
                    time.sleep(1)  # Show result message briefly
                    
            except Exception as e:
                logging.error(f"Favorites error: {str(e)}")
                self.console.print(f"\n[red]Error managing favorites: {str(e)}[/red]")
                time.sleep(2)

if __name__ == "__main__":
    try:
        app = CatchStream()
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(app.initialize_components())
        loop.run_until_complete(app.main())
    except RuntimeError as e:
        if "another loop is running" in str(e):
            print("Error: Another event loop is already running.")
        else:
            raise
    except KeyboardInterrupt:
        print("\nProgram terminated by user.")
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
    finally:
        try:
            loop.close()
        except:
            pass
