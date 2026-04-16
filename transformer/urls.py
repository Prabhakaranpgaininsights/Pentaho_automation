"""
"""
from django.urls import path
from .views import upload_tr,upload_job,table_builder,generate_sql,create_table,generate_pentaho_files,generate_pentaho,dashboard,dashboard_stats,dashboard_clear_history,history_list,history_detail,table_join,test_db_connection,table_joins,table_builder_execute_sql,login_view,logout_view,generate_sql_preview,downloads_list,downloads_stats,download_zip,download_file,delete_file,delete_all_files,save_api_settings,get_api_settings,test_api_key,config_api,save_preferences,get_preferences,history_only,callback,home,console
from django.contrib.auth import views as auth_views
from django.conf import settings
from django.conf.urls.static import static

urlpatterns = [
    path("home", home, name="home"),
    path("dashboard/", dashboard, name="dashboard"),
    path('',  login_view,  name='pentaho_login'),
    path('logout/', logout_view, name='logout'),

    # Built-in password reset (requires email setup in settings.py)
    path('password-reset/',
         auth_views.PasswordResetView.as_view(template_name='password_reset.html'),
         name='password_reset'),
    path('password-reset/done/',
         auth_views.PasswordResetDoneView.as_view(template_name='password_reset_done.html'),
         name='password_reset_done'),
    path('password-reset/<uidb64>/<token>/',
         auth_views.PasswordResetConfirmView.as_view(template_name='password_reset_confirm.html'),
         name='password_reset_confirm'),
    path('password-reset/complete/',
         auth_views.PasswordResetCompleteView.as_view(template_name='password_reset_complete.html'),
         name='password_reset_complete'),
         
    path("dashboard/stats/", dashboard_stats, name="dashboard_stats"),
    path("history-only/", history_only, name="history_only"),
    path("dashboard/history/clear/", dashboard_clear_history, name="dashboard_clear_history"),
    path('upload_tr/', upload_tr, name='upload_tr'),
    path('upload/job/', upload_job, name='upload_job'),
    path("table-builder/", table_builder, name="table_builder"),
    path("generate-sql/",  generate_sql,  name="generate_sql"),
    path("generate-sql-preview/",  generate_sql_preview,  name="generate_sql_preview"),
    path("create-table/",  create_table,  name="create_table"),
    path("generate-pentaho/",  generate_pentaho,  name="generate_pentaho"),
    path("generate_pentaho_files/",  generate_pentaho_files,  name="generate_pentaho_files"),
    path("dashboard/history/",  history_list, name="history_list"),
    path("dashboard/history/<int:pk>/",  history_detail, name="history_detail"),
    path("table-join/", table_join, name="table_join"),
    path("table-joins/", table_joins, name="table_joins"),
    path("test-connection/", test_db_connection, name="test_connection"),
    path("table-builder-execute_sql/", table_builder_execute_sql, name="table_builder_execute_sql"),

    path('downloads/', downloads_list,   name='downloads_list'),
     path('downloads/stats/', downloads_stats,  name='downloads_stats'),
     path('downloads/zip/', download_zip,     name='download_zip'),
     path('downloads/<int:file_id>/download/', download_file, name='download_file'),
     path('downloads/<int:file_id>/delete/', delete_file,   name='delete_file'),
     path('downloads/delete-all/', delete_all_files, name='delete_all_files'),

     path('save-api-settings/', save_api_settings, name='save_api_settings'),
     path('get-api-settings/', get_api_settings, name='get_api_settings'),
     path("test-api-key/", test_api_key, name="test_api_key"),

     path("config-api/", config_api, name="config_api"),
     path("save-preferences/", save_preferences, name="save_preferences"),
     path("get-preferences/", get_preferences, name="get_preferences"),

     path('speech/', callback, name='receive_speech'),

     path('console/', console, name='console'),
]+ static(settings.STATIC_URL, document_root=settings.STATICFILES_DIRS[0])
urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)
