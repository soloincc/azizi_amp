from django.db import models
from jsonfield import JSONField


class BaseTable(models.Model):
    """
    Base abstract table to be inherited by all other tables
    """
    date_created = models.DateTimeField(auto_now=True)
    date_modified = models.DateTimeField(auto_now=True)

    class Meta:
        abstract = True


class Model(BaseTable):
    """
    Defines the structure of the model table
    """
    # the columns for the tables
    model_name = models.CharField(max_length=100)

    class Meta:
        db_table = '__models'

    def publish(self):
        self.save()


class Attribute(BaseTable):
    """
    Defines the structure of the attributes table
    """
    _name = models.CharField(max_length=100)
    _type = models.CharField(max_length=50)
    _size = models.SmallIntegerField()
    _model = models.ForeignKey('Model', on_delete=models.CASCADE)

    class Meta:
        db_table = '__attributes'

    def publish(self):
        self.save()


class ODKFormGroup(BaseTable):
    # define the dictionary structure
    order_index = models.IntegerField(unique=True)
    group_name = models.CharField(max_length=100, unique=True)
    comments = models.CharField(max_length=1000, null=True)

    class Meta:
        db_table = 'form_groups'

    def publish(self):
        self.save()

    def get_id(self):
        return self.id


class ODKForm(BaseTable):
    # Define the structure of the form table
    form_id = models.IntegerField(unique=True, db_index=True)
    form_group = models.ForeignKey(ODKFormGroup, null=True)
    form_name = models.CharField(max_length=200, unique=True)
    full_form_id = models.CharField(max_length=200, unique=True)
    structure = JSONField(null=True)
    processed_structure = JSONField(null=True)
    auto_update = models.BooleanField(default=False)
    is_source_deleted = models.BooleanField(default=False)

    class Meta:
        db_table = 'odkform'

    def publish(self):
        self.save()

    def get_id(self):
        return self.id


class RawSubmissions(BaseTable):
    # Define the structure of the submission table
    form = models.ForeignKey(ODKForm)
    uuid = models.CharField(max_length=100, unique=True, db_index=True)
    submission_time = models.CharField(max_length=100)
    is_processed = models.BooleanField(default=0)
    is_modified = models.BooleanField(default=0)
    raw_data = JSONField()

    class Meta:
        db_table = 'raw_submissions'

    def publish(self):
        self.save()

    def get_id(self):
        return self.uuid


class FormViews(BaseTable):
    # Define the structure of the submission table
    form = models.ForeignKey(ODKForm)
    view_name = models.CharField(max_length=100, unique=True, db_index=True)
    proper_view_name = models.CharField(max_length=100, db_index=True)
    structure = JSONField()

    class Meta:
        db_table = 'form_views'

    def publish(self):
        self.save()

    def get_id(self):
        return self.view_name


class ViewTablesLookup(BaseTable):
    # Define the structure of the views that will be generated
    view = models.ForeignKey(FormViews)
    table_name = models.CharField(max_length=250, unique=True, db_index=True)
    proper_table_name = models.CharField(max_length=250, null=True, db_index=True)
    hashed_name = models.CharField(max_length=100, unique=True, db_index=True)

    class Meta:
        db_table = 'views_table_lookup'

    def publish(self):
        self.save()

    def get_id(self):
        return self.table_name


class ViewsData(BaseTable):
    # Define the structure of the submission table
    view = models.ForeignKey(FormViews)
    raw_data = JSONField()

    class Meta:
        db_table = 'views_data'

    def publish(self):
        self.save()

    def get_id(self):
        return self.view


class ImagesLookup(models.Model):
    # Define the structure of the submission table
    filename = models.CharField(max_length=50, unique=True, db_index=True)
    species = models.CharField(max_length=50, null=True)
    breed = models.CharField(max_length=50, null=True)
    country = models.CharField(max_length=80, null=True)

    class Meta:
        db_table = 'images_lookup'

    def publish(self):
        self.save()

    def get_id(self):
        return self.filename


class DictionaryItems(BaseTable):
    # define the dictionary structure
    form_group = models.CharField(max_length=100, db_index=True)
    parent_node = models.CharField(max_length=100, db_index=True, null=True)
    t_key = models.CharField(max_length=100, db_index=True)
    t_locale = models.CharField(max_length=50)
    t_type = models.CharField(max_length=30, db_index=True)
    t_value = models.CharField(max_length=1000)

    class Meta:
        unique_together = ('form_group', 'parent_node', 't_key')
        db_table = 'dictionary_items'

    def publish(self):
        self.save()

    def get_id(self):
        return self.t_key


class FormMappings(BaseTable):
    # define the dictionary structure
    form_group = models.CharField(max_length=50, db_index=True)
    form_question = models.CharField(max_length=100)
    dest_table_name = models.CharField(max_length=100, db_index=True)
    dest_column_name = models.CharField(max_length=50, db_index=True)
    # @todo Add proper handling of choices like the enum in mysql
    odk_question_type = models.CharField(max_length=50)     # This will be, single_select, multiple_select, integer,
    db_question_type = models.CharField(max_length=50)     # This will be, single_select, multiple_select, integer,
    # @todo change this to a proper validator field
    ref_table_name = models.CharField(max_length=100, null=True)
    ref_column_name = models.CharField(max_length=50, null=True)
    validation_regex = models.CharField(max_length=100, null=True)
    is_record_identifier = models.BooleanField(default=False)
    is_null = models.SmallIntegerField(null=True)
    is_lookup_field = models.NullBooleanField(default=False, null=True)
    use_current_time = models.NullBooleanField(default=False, null=True)

    class Meta:
        unique_together = ('form_group', 'form_question', 'dest_table_name', 'dest_column_name')
        db_table = 'mappings_table'

    def publish(self):
        self.save()

    def get_id(self):
        return self.t_key


class ProcessingErrors(BaseTable):
    # define the dictionary structure
    err_code = models.IntegerField(db_index=True)
    err_message = models.TextField()
    data_uuid = models.CharField(max_length=100, db_index=True)
    err_comments = models.CharField(max_length=1000, null=True)
    is_resolved = models.BooleanField(default=False)

    class Meta:
        # unique_together = ('form_group', 'form_question', 'dest_table_name', 'dest_column_name')
        db_table = 'processing_errors'

    def publish(self):
        self.save()

    def get_id(self):
        return self.t_key
