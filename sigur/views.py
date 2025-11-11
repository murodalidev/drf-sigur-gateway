from django.shortcuts import get_object_or_404

from rest_framework.response import Response
from rest_framework import generics, status
from rest_framework.exceptions import APIException
from rest_framework.views import APIView
from rest_framework_api_key.permissions import HasAPIKey

from .models import Sql
from .serializers import SqlSerializer
from .services.mysql import (
    MySQLConfigurationError,
    MySQLConnectionError,
    MySQLDatabase,
    MySQLExecutionError,
    execute_raw_sql,
)


class HealthCheckView(APIView):
    permission_classes = [HasAPIKey]

    def get(self, request):
        return Response({'message': 'Sigur gateway is operational.'})


class SqlListView(generics.ListAPIView):
    queryset = Sql.objects.filter(is_active=True).order_by('name')
    serializer_class = SqlSerializer
    permission_classes = [HasAPIKey]


class SqlRetrieveView(APIView):
    permission_classes = [HasAPIKey]

    def get(self, request, path: str):
        sql_object = get_object_or_404(Sql, path=path, is_active=True)

        try:
            target_db = MySQLDatabase.from_value(sql_object.database)
            data = execute_raw_sql(sql_object.raw, target=target_db)
        except MySQLConfigurationError as exc:
            raise APIException(str(exc))
        except MySQLConnectionError as exc:
            raise APIException(str(exc))
        except MySQLExecutionError as exc:
            raise APIException(str(exc))

        return Response({'path': sql_object.path, 'data': data}, status=status.HTTP_200_OK)


