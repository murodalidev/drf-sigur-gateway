from django.contrib import admin

# Register your models here.
from .models import Sql


@admin.register(Sql)
class SqlAdmin(admin.ModelAdmin):
    list_display = ['name', 'path', 'is_active', 'created_at']
    list_filter = ['is_active']
    search_fields = ['name', 'path']
    list_editable = ['is_active']
    date_hierarchy = 'created_at'
    prepopulated_fields = {'path': ('name',)}
    readonly_fields = ['created_at']