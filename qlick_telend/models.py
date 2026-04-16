"""
models.py — Pentaho Generator
─────────────────────────────
Run after adding this file:
    python manage.py makemigrations
    python manage.py migrate
"""

from django.db import models
from django.utils import timezone
from django.contrib.auth.models import User

class GenerationLog(models.Model):
    """One row per generated file (.ktr or .kjb)."""

    FILE_TYPE_CHOICES = [
        ("ktr",  "Transformation (.ktr)"),
        ("kjb",  "Job (.kjb)"),
    ]

    STATUS_CHOICES = [
        ("ok",    "Success"),
        ("error", "Error"),
    ]

    # File info
    filename      = models.CharField(max_length=255)
    file_type     = models.CharField(max_length=4, choices=FILE_TYPE_CHOICES)
    step_count    = models.PositiveIntegerField(default=0)

    # Claude API metrics
    input_tokens  = models.PositiveIntegerField(default=0)
    output_tokens = models.PositiveIntegerField(default=0)
    duration_ms   = models.PositiveIntegerField(default=0)   # milliseconds

    # Result
    status        = models.CharField(max_length=8, choices=STATUS_CHOICES, default="ok")
    error_message = models.TextField(blank=True, default="")

    # Meta
    created_at    = models.DateTimeField(default=timezone.now)

    class Meta:
        ordering = ["-created_at"]
        verbose_name      = "Generation Log"
        verbose_name_plural = "Generation Logs"

    def __str__(self):
        return f"{self.filename} [{self.file_type.upper()}] — {self.status}"

    @property
    def total_tokens(self):
        return self.input_tokens + self.output_tokens

    @property
    def duration_s(self):
        return f"{self.duration_ms / 1000:.1f}s" if self.duration_ms else "—"


class GeneratedFile(models.Model):
    FILE_TYPES = [('ktr', 'KTR'), ('kjb', 'KJB'), ('sql', 'SQL'), ('zip', 'ZIP')]

    name        = models.CharField(max_length=255)          # filename without extension
    file_type   = models.CharField(max_length=10, choices=FILE_TYPES)
    file        = models.FileField(upload_to='generated_files/')  # stored in MEDIA
    size_bytes  = models.PositiveIntegerField(default=0)
    created_at  = models.DateTimeField(auto_now_add=True)
    source      = models.CharField(max_length=50, default='generator')  # generator / join_builder / table_builder
    log         = models.ForeignKey('GenerationLog', null=True, blank=True,
                                    on_delete=models.SET_NULL, related_name='files')

    class Meta:
        ordering = ['-created_at']

    def __str__(self):
        return f"{self.name}.{self.file_type}"

    def filename(self):
        return f"{self.name}.{self.file_type}"

    def size_display(self):
        b = self.size_bytes
        if b >= 1024 * 1024: return f"{b/1024/1024:.1f} MB"
        if b >= 1024:        return f"{b/1024:.1f} KB"
        return f"{b} B"
    

class UserApiSettings(models.Model):
    user = models.OneToOneField(
        User,
        on_delete=models.CASCADE,
        related_name='qlick_user_api_settings'  
    )
    api_key    = models.CharField(max_length=255, blank=True)
    model      = models.CharField(max_length=100, default="claude-opus-4-20250514")
    max_tokens = models.IntegerField(default=16000)
    timeout    = models.IntegerField(default=180)

    def __str__(self):
        return f"{self.user.username} API Settings"
    

class UserPreference(models.Model):
    refresh = models.IntegerField(default=60)
    toast = models.BooleanField(default=True)
    auto_download = models.BooleanField(default=True)
    debug = models.BooleanField(default=False)
    history_limit = models.IntegerField(default=50)
    folder_name = models.CharField(max_length=255, blank=True)

    def __str__(self):
        return "User Preferences"