# This is an auto-generated Django model module.
# You'll have to do the following manually to clean this up:
#   * Rearrange models' order
#   * Make sure each model has one field with primary_key=True
#   * Make sure each ForeignKey and OneToOneField has `on_delete` set to the desired behavior
#   * Remove `managed = False` lines if you wish to allow Django to create, modify, and delete the table
# Feel free to rename the models, but don't rename db_table values or field names.
from django.db import models


class AuthGroup(models.Model):
    name = models.CharField(unique=True, max_length=150)

    class Meta:
        managed = False
        db_table = 'auth_group'


class AuthGroupPermissions(models.Model):
    group = models.ForeignKey(AuthGroup, models.DO_NOTHING)
    permission = models.ForeignKey('AuthPermission', models.DO_NOTHING)

    class Meta:
        managed = False
        db_table = 'auth_group_permissions'
        unique_together = (('group', 'permission'),)


class AuthPermission(models.Model):
    content_type = models.ForeignKey('DjangoContentType', models.DO_NOTHING)
    codename = models.CharField(max_length=100)
    name = models.CharField(max_length=255)

    class Meta:
        managed = False
        db_table = 'auth_permission'
        unique_together = (('content_type', 'codename'),)


class AuthUser(models.Model):
    password = models.CharField(max_length=128)
    last_login = models.DateTimeField(blank=True)
    is_superuser = models.BooleanField()
    username = models.CharField(unique=True, max_length=150)
    last_name = models.CharField(max_length=150)
    email = models.CharField(max_length=254)
    is_staff = models.BooleanField()
    is_active = models.BooleanField()
    date_joined = models.DateTimeField()
    first_name = models.CharField(max_length=150)

    class Meta:
        managed = False
        db_table = 'auth_user'


class AuthUserGroups(models.Model):
    user = models.ForeignKey(AuthUser, models.DO_NOTHING)
    group = models.ForeignKey(AuthGroup, models.DO_NOTHING)

    class Meta:
        managed = False
        db_table = 'auth_user_groups'
        unique_together = (('user', 'group'),)


class AuthUserUserPermissions(models.Model):
    user = models.ForeignKey(AuthUser, models.DO_NOTHING)
    permission = models.ForeignKey(AuthPermission, models.DO_NOTHING)

    class Meta:
        managed = False
        db_table = 'auth_user_user_permissions'
        unique_together = (('user', 'permission'),)


class Characters(models.Model):
    pk = models.CompositePrimaryKey('movie_id', 'person_id', 'character_name')
    movie = models.ForeignKey('Movies', models.DO_NOTHING, blank=True)
    person = models.ForeignKey('Persons', models.DO_NOTHING, blank=True)
    character_name = models.TextField(blank=True)

    class Meta:
        managed = False
        db_table = 'characters'


class Directors(models.Model):
    pk = models.CompositePrimaryKey('movie_id', 'person_id')
    movie = models.ForeignKey('Movies', models.DO_NOTHING, blank=True)
    person = models.ForeignKey('Persons', models.DO_NOTHING, blank=True)

    class Meta:
        managed = False
        db_table = 'directors'


class DjangoAdminLog(models.Model):
    object_id = models.TextField(blank=True)
    object_repr = models.CharField(max_length=200)
    action_flag = models.PositiveSmallIntegerField()
    change_message = models.TextField()
    content_type = models.ForeignKey('DjangoContentType', models.DO_NOTHING, blank=True)
    user = models.ForeignKey(AuthUser, models.DO_NOTHING)
    action_time = models.DateTimeField()

    class Meta:
        managed = False
        db_table = 'django_admin_log'


class DjangoContentType(models.Model):
    app_label = models.CharField(max_length=100)
    model = models.CharField(max_length=100)

    class Meta:
        managed = False
        db_table = 'django_content_type'
        unique_together = (('app_label', 'model'),)


class DjangoMigrations(models.Model):
    app = models.CharField(max_length=255)
    name = models.CharField(max_length=255)
    applied = models.DateTimeField()

    class Meta:
        managed = False
        db_table = 'django_migrations'


class DjangoSession(models.Model):
    session_key = models.CharField(primary_key=True, max_length=40)
    session_data = models.TextField()
    expire_date = models.DateTimeField()

    class Meta:
        managed = False
        db_table = 'django_session'


class Genres(models.Model):
    pk = models.CompositePrimaryKey('movie_id', 'genre')
    movie = models.ForeignKey('Movies', models.DO_NOTHING, blank=True)
    genre = models.TextField(blank=True)

    class Meta:
        managed = False
        db_table = 'genres'


class Movies(models.Model):
    movie_id = models.TextField(primary_key=True, blank=True)
    title = models.TextField(blank=True)
    original_title = models.TextField(blank=True)
    year = models.IntegerField(blank=True)
    runtime = models.IntegerField(blank=True)

    class Meta:
        managed = False
        db_table = 'movies'


class Persons(models.Model):
    person_id = models.TextField(primary_key=True, blank=True)
    name = models.TextField(blank=True)
    birth_year = models.IntegerField(blank=True)
    death_year = models.IntegerField(blank=True)

    class Meta:
        managed = False
        db_table = 'persons'


class Principals(models.Model):
    pk = models.CompositePrimaryKey('movie_id', 'ordering', 'person_id', 'category')
    movie = models.ForeignKey(Movies, models.DO_NOTHING, blank=True)
    ordering = models.IntegerField(blank=True)
    person = models.ForeignKey(Persons, models.DO_NOTHING, blank=True)
    category = models.TextField(blank=True)
    job = models.TextField(blank=True)

    class Meta:
        managed = False
        db_table = 'principals'


class Professions(models.Model):
    pk = models.CompositePrimaryKey('person_id', 'job_name')
    person = models.ForeignKey(Persons, models.DO_NOTHING, blank=True)
    job_name = models.TextField(blank=True)

    class Meta:
        managed = False
        db_table = 'professions'


class Ratings(models.Model):
    movie = models.OneToOneField(Movies, models.DO_NOTHING, primary_key=True, blank=True)
    average_rating = models.FloatField(blank=True)
    num_votes = models.IntegerField(blank=True)

    class Meta:
        managed = False
        db_table = 'ratings'


class Titles(models.Model):
    title_id = models.AutoField(primary_key=True, blank=True)
    movie = models.ForeignKey(Movies, models.DO_NOTHING, blank=True)
    ordering = models.IntegerField(blank=True)
    title = models.TextField(blank=True)
    region = models.TextField(blank=True)
    language = models.TextField(blank=True)
    types = models.TextField(blank=True)
    attributes = models.TextField(blank=True)
    is_original_title = models.IntegerField(blank=True)

    class Meta:
        managed = False
        db_table = 'titles'


class Writers(models.Model):
    pk = models.CompositePrimaryKey('movie_id', 'person_id')
    movie = models.ForeignKey(Movies, models.DO_NOTHING, blank=True)
    person = models.ForeignKey(Persons, models.DO_NOTHING, blank=True)

    class Meta:
        managed = False
        db_table = 'writers'
