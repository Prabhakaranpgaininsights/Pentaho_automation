# ═══════════════════════════════════════════════════════════════════════════════
# ADD THESE TO YOUR EXISTING urls.py
# ═══════════════════════════════════════════════════════════════════════════════
#
# 1. Add these imports at the top of urls.py:
#
# from .views_qlik_talend import (
#     qlik_generator_page, generate_qlik_files, upload_qlik,
#     talend_generator_page, generate_talend_files, upload_talend,
# )
#
# 2. Add these paths inside urlpatterns = [...]:

from django.urls import path

from .views import (          # ← NEW
    # qlik_generator_page, generate_qlik_files, upload_qlik,
    talend_generator_page, generate_talend_files, upload_talend,talend_login,talend_logout,talend_dashboard,
    dashboard_talend_stats
)

urlpatterns = [
    path('login/talend/',  talend_login,  name='talend_login'),
    path('talend/dashboard/',  talend_dashboard,  name='talend_dashboard'),
    path('logout/talend/',  talend_logout,  name='talend_logout'),
    path("dashboard/talend-stats/", dashboard_talend_stats, name="dashboard_talend_stats"),
    # path("",                  qlik_generator_page,   name="qlik_generator"),
    # path("qlik/generate/",         generate_qlik_files,   name="generate_qlik_files"),
    # path("qlik/upload/",           upload_qlik,           name="upload_qlik"),

    # ── Talend Generator ───────────────────────────────────────────────────
    path("talend/",                talend_generator_page, name="talend_generator"),
    path("talend/generate/",       generate_talend_files, name="generate_talend_files"),
    path("talend/upload/",         upload_talend,         name="upload_talend"),

]
