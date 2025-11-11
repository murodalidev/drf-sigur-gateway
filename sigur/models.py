from django.core.validators import validate_slug
from django.db import models


class Sql(models.Model):
    class DatabaseChoices(models.TextChoices):
        MAIN = 'main', 'Main'
        LOG = 'log', 'Log'

    name = models.CharField(max_length=255)
    path = models.CharField(max_length=255, validators=[validate_slug], unique=True)
    raw = models.TextField()
    description = models.TextField(blank=True, null=True)
    database = models.CharField(
        max_length=16,
        choices=DatabaseChoices.choices,
        default=DatabaseChoices.MAIN,
    )
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return self.name