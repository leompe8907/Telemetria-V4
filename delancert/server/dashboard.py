from __future__ import annotations

from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from delancert.utils.api_key_permission import HasTelemetryReadApiKey
from delancert.utils.api_key_authentication import TelemetryApiKeyAuthentication

from delancert.analytics.common import parse_date_range
from delancert.analytics.overview import overview
from delancert.analytics.channels import top_channels, channel_audience, peak_hours_by_channel
from delancert.analytics.temporal import temporal
from delancert.analytics.users import user_profile, user_range
from delancert.analytics.users_general import users_general


def _get_range(request):
    start = request.query_params.get("start")
    end = request.query_params.get("end")
    return parse_date_range(start, end)


class DashboardOverviewView(APIView):
    permission_classes = [HasTelemetryReadApiKey]
    authentication_classes = [TelemetryApiKeyAuthentication]

    def get(self, request):
        try:
            range_ = _get_range(request)
            return Response(overview(range_), status=status.HTTP_200_OK)
        except ValueError as e:
            return Response({"error": str(e)}, status=status.HTTP_400_BAD_REQUEST)


class DashboardTopChannelsView(APIView):
    permission_classes = [HasTelemetryReadApiKey]
    authentication_classes = [TelemetryApiKeyAuthentication]

    def get(self, request):
        try:
            range_ = _get_range(request)
            limit = int(request.query_params.get("limit", 10))
            return Response(top_channels(range_, limit=limit), status=status.HTTP_200_OK)
        except ValueError as e:
            return Response({"error": str(e)}, status=status.HTTP_400_BAD_REQUEST)


class DashboardChannelAudienceView(APIView):
    permission_classes = [HasTelemetryReadApiKey]
    authentication_classes = [TelemetryApiKeyAuthentication]

    def get(self, request):
        try:
            range_ = _get_range(request)
            return Response(channel_audience(range_), status=status.HTTP_200_OK)
        except ValueError as e:
            return Response({"error": str(e)}, status=status.HTTP_400_BAD_REQUEST)


class DashboardPeakHoursByChannelView(APIView):
    permission_classes = [HasTelemetryReadApiKey]
    authentication_classes = [TelemetryApiKeyAuthentication]

    def get(self, request):
        try:
            range_ = _get_range(request)
            channel = request.query_params.get("channel")
            return Response(peak_hours_by_channel(range_, channel=channel), status=status.HTTP_200_OK)
        except ValueError as e:
            return Response({"error": str(e)}, status=status.HTTP_400_BAD_REQUEST)


class DashboardTemporalView(APIView):
    permission_classes = [HasTelemetryReadApiKey]
    authentication_classes = [TelemetryApiKeyAuthentication]

    def get(self, request):
        try:
            range_ = _get_range(request)
            period = request.query_params.get("period", "daily")
            return Response(temporal(range_, period=period), status=status.HTTP_200_OK)
        except ValueError as e:
            return Response({"error": str(e)}, status=status.HTTP_400_BAD_REQUEST)


class DashboardUserProfileView(APIView):
    permission_classes = [HasTelemetryReadApiKey]
    authentication_classes = [TelemetryApiKeyAuthentication]

    def get(self, request, subscriber_code: str):
        try:
            range_ = _get_range(request)
            return Response(user_profile(subscriber_code, range_), status=status.HTTP_200_OK)
        except ValueError as e:
            return Response({"error": str(e)}, status=status.HTTP_400_BAD_REQUEST)


class DashboardUserRangeView(APIView):
    permission_classes = [HasTelemetryReadApiKey]
    authentication_classes = [TelemetryApiKeyAuthentication]

    def get(self, request, subscriber_code: str):
        try:
            range_ = _get_range(request)
            if range_ is None:
                raise ValueError("start y end son obligatorios para este endpoint.")
            return Response(user_range(subscriber_code, range_), status=status.HTTP_200_OK)
        except ValueError as e:
            return Response({"error": str(e)}, status=status.HTTP_400_BAD_REQUEST)


class DashboardUsersGeneralView(APIView):
    permission_classes = [HasTelemetryReadApiKey]
    authentication_classes = [TelemetryApiKeyAuthentication]

    def get(self, request):
        try:
            range_ = _get_range(request)
            return Response(users_general(range_), status=status.HTTP_200_OK)
        except ValueError as e:
            return Response({"error": str(e)}, status=status.HTTP_400_BAD_REQUEST)

