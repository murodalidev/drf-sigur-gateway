from django.urls import path

from .views import HealthCheckView, SqlListView, SqlRetrieveView

urlpatterns = [
    path('health/', HealthCheckView.as_view(), name='health'),
    path('sigur/', SqlListView.as_view(), name='data-list'),
    path('sigur/<slug:path>/', SqlRetrieveView.as_view(), name='data-detail'),
]

