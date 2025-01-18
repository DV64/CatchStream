<div align="center">

# 🎥 CatchStream

### Advanced Video Downloader with Terminal UI

[![Python 3.8+](https://img.shields.io/badge/Python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Platform](https://img.shields.io/badge/Platform-Windows%20%7C%20Linux%20%7C%20macOS%20%7C%20Android-green.svg)](https://github.com/DV64/CatchStream)

[Features](#features) • [Installation](#installation) • [Usage](#usage) • [Configuration](#configuration) • [FAQ](#faq)

</div>

---

## 📋 Features

### Core Features
- Multi-platform support (Windows, Linux, macOS, Android)
- Concurrent downloads with queue management
- Multiple quality presets (480p, 720p, 1080p)
- Progress tracking with detailed statistics
- Download resume capability
- Playlist support

### Advanced Features
- Video processing (compression, watermark, trim)
- Audio extraction
- Subtitle download
- Thumbnail download
- Format conversion
- Archive management
- Favorites system

### User Experience
- Rich terminal interface
- Real-time progress tracking
- Download statistics
- Custom themes
- Notifications

## 📋 Requirements

- Python 3.8 or higher
- FFmpeg
- Internet connection

## ⚡ Quick Start

1. **Clone Repository**
```bash
git clone https://github.com/DV64/CatchStream.git
cd CatchStream
```

2. **Install Dependencies**
```bash
pip install -r requirements.txt
```

3. **Run Application**
```bash
python CatchStream.py
```

## 🔧 Installation Guide

### Windows
```powershell
# Install FFmpeg using winget or chocolatey
winget install FFmpeg
# OR
choco install ffmpeg
```

### Linux (Ubuntu/Debian)
```bash
sudo apt update
sudo apt install python3 python3-pip ffmpeg
```

### macOS
```bash
# Using Homebrew
brew install python ffmpeg
```

### Android (Termux)
```bash
pkg update
pkg install python ffmpeg
```

## 💻 Usage

### Single Video Download
1. Launch CatchStream
2. Select option 1 (Single Video)
3. Enter video URL
4. Choose quality preset
5. Wait for download to complete

### Playlist Download
1. Select option 2 (Playlist)
2. Enter playlist URL
3. Choose videos to download
4. Select quality preset

### Batch Download
1. Select option 3 (Batch)
2. Enter multiple URLs
3. Set quality preferences

## ⚙️ Configuration

Edit `config.json` to customize:
```json
{
    "theme": "cyan",
    "auto_cleanup": true,
    "log_retention_days": 30,
    "audio_bitrate": "192k",
    "compression_preset": "medium",
    "download_path": "~/Downloads",
    "default_quality": "high",
    "max_concurrent_downloads": 3,
    "auto_archive": false,
    "notifications": true,
    "thumbnail_quality": "high",
    "interface": {
        "show_file_size": true,
        "show_eta": true,
        "progress_bar_style": "detailed",
        "auto_clear_completed": true
    },
    "performance": {
        "cache_size": 100,
        "chunk_size": 1024,
        "buffer_size": 8192,
        "max_retries": 5
    },
    "downloads": {
        "auto_resume": true,
        "verify_ssl": true,
        "timeout": 30,
        "rate_limit": 0
    }
}
```

### Configuration Options

- **Basic Settings**
  - `theme`: UI color theme (default: "cyan")
  - `auto_cleanup`: Automatically clean temporary files (default: true)
  - `log_retention_days`: Days to keep log files (default: 30)
  - `download_path`: Default download location
  - `default_quality`: Default video quality (low/medium/high)
  - `max_concurrent_downloads`: Maximum parallel downloads

- **Media Settings**
  - `audio_bitrate`: Audio quality for extractions
  - `compression_preset`: Video compression level
  - `thumbnail_quality`: Quality of downloaded thumbnails

- **Interface Options**
  - `show_file_size`: Display file sizes
  - `show_eta`: Show estimated time remaining
  - `progress_bar_style`: Progress bar appearance
  - `auto_clear_completed`: Remove completed downloads from view

- **Performance Tuning**
  - `cache_size`: Size of video info cache
  - `chunk_size`: Download chunk size
  - `buffer_size`: Buffer size for processing
  - `max_retries`: Maximum retry attempts

- **Download Settings**
  - `auto_resume`: Resume interrupted downloads
  - `verify_ssl`: Verify SSL certificates
  - `timeout`: Connection timeout in seconds
  - `rate_limit`: Download speed limit (0 for unlimited)

## 📝 Project Structure

```
CatchStream/
├── CatchStream.py        # Main application file
├── config.json          # Application configuration
├── requirements.txt     # Python dependencies
├── VERSION             # Version information
├── CHANGELOG.md        # Version history and changes
├── LICENSE            # MIT License
├── README.md          # Documentation
├── downloads/         # Downloaded videos directory
├── temp/             # Temporary files
├── cache/            # Cache directory
├── archive/          # Archived downloads
└── logs/             # Application logs
    └── catchstream_YYYYMMDD.log
```

### Directory Structure
- `downloads/`: Storage for downloaded videos
- `temp/`: Temporary files during processing
- `cache/`: Cached video information
- `archive/`: Long-term storage for completed downloads
- `logs/`: Application logs with daily rotation

### Core Files
- `CatchStream.py`: Main application logic and UI
- `config.json`: User configuration and settings
- `VERSION`: Current version number
- `CHANGELOG.md`: Version history and feature updates
- `requirements.txt`: Required Python packages

## 🤝 Contributing

1. Fork repository
2. Create feature branch
3. Commit changes
4. Push to branch
5. Open Pull Request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 📧 Contact

DV64 - [@DV64](https://github.com/DV64)

Project Link: [https://github.com/DV64/CatchStream](https://github.com/DV64/CatchStream)

---
<div align="center">
Made by DV64
</div>
```
