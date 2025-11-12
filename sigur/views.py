from typing import Any, Dict

from django.shortcuts import get_object_or_404

from rest_framework.response import Response
from rest_framework import generics, status
from rest_framework.exceptions import APIException, ValidationError
from rest_framework.views import APIView
from rest_framework_api_key.permissions import HasAPIKey

from .models import Sql
from .serializers import SqlSerializer
from .services.mysql import (
    MySQLConfigurationError,
    MySQLConnectionError,
    MySQLDatabase,
    MySQLExecutionError,
    MySQLParameterError,
    analyse_placeholders,
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
        required_params = []
        positional_params_count = 0

        try:
            _, named_params, positional_params_count = analyse_placeholders(sql_object.raw)
            required_params = sorted(named_params)
            target_db = MySQLDatabase.from_value(sql_object.database)

            if positional_params_count:
                params_sequence = list(request.query_params.values())
                params = tuple(params_sequence) if params_sequence else None
            else:
                query_params = request.query_params.dict()
                params = query_params or None

            data = execute_raw_sql(sql_object.raw, params=params, target=target_db)
        except MySQLConfigurationError as exc:
            raise APIException(str(exc))
        except MySQLConnectionError as exc:
            raise APIException(str(exc))
        except MySQLParameterError as exc:
            detail: Dict[str, Any] = {
                'detail': str(exc),
                'required_params': required_params,
                'positional_params_count': positional_params_count,
            }
            if getattr(exc, 'missing_params', None):
                detail['missing_params'] = exc.missing_params
            raise ValidationError(detail)
        except MySQLExecutionError as exc:
            raise APIException(str(exc))

        return Response(
            {
                'path': sql_object.path,
                'required_params': required_params,
                'positional_params_count': positional_params_count,
                'data': data,
            },
            status=status.HTTP_200_OK,
        )


