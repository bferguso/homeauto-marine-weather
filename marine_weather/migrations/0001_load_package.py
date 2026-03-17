from django.db import migrations
from django.core.management import call_command
from django.conf import settings
import os


class Migration(migrations.Migration):
    dependencies = [
        ("models", "12586_tile_cardinality_check"),
    ]

    create_resource_proxy_views_sql = """
        select __arches_create_resource_model_views(graphid)
            from graphs
            where isresource = true
              and publicationid is not null
              and slug != 'arches_system_settings';
        """

    @staticmethod
    def load_package(app, someethingelse):
        call_command(
            "packages",
            operation="load_package",
            source=f"{settings.APP_ROOT}/pkg",
            yes=True,
        )

    @staticmethod
    def create_cache(app, somethingelse):
        call_command("createcachetable")

    operations = [
        migrations.RunPython(create_cache, migrations.RunPython.noop),
        migrations.RunPython(load_package, migrations.RunPython.noop),
        migrations.RunSQL(
            create_resource_proxy_views_sql,
            migrations.RunSQL.noop,
        ),
    ]
