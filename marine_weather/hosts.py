import re
from django_hosts import patterns, host

host_patterns = patterns(
    "",
    host(re.sub(r"_", r"-", r"marine_weather"), "marine_weather.urls", name="marine_weather"),
)
