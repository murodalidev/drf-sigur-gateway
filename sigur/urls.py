from django.urls import path

from .views import HealthCheckView, SqlListView, SqlRetrieveView

urlpatterns = [
    path('health/', HealthCheckView.as_view(), name='health'),
    path('sql/', SqlListView.as_view(), name='sql-list'),
    path('sql/<slug:path>/', SqlRetrieveView.as_view(), name='sql-detail'),
]

