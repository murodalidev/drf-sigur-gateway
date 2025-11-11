from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('sigur', '0002_sql_description'),
    ]

    operations = [
        migrations.AddField(
            model_name='sql',
            name='database',
            field=models.CharField(
                choices=[('main', 'Main'), ('log', 'Log')],
                default='main',
                max_length=16,
            ),
        ),
    ]

