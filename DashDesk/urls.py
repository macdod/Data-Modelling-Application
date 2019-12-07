from django.conf.urls import url,include
from django.contrib import admin

urlpatterns = [
    url(r'^admin/', admin.site.urls),
    url(r'^dash10/', include('dash10.urls')),
    url(r'^temp/', include('temp.urls')),
]
