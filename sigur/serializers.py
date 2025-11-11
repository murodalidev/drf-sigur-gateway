from rest_framework import serializers

from .models import Sql


class SqlSerializer(serializers.ModelSerializer):
    class Meta:
        model = Sql
        fields = ['name', 'path', 'raw', 'description', 'database']