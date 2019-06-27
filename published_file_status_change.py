import re
import os
import abc
import sys
import subprocess

##########
# Shotgun
##########


SCRIPT_KEY = 'some_key'
SCRIPT_NAME = 'published_file_status_change'

RE_FBX_PUBLISH_TEMPLATE = r'(?P<version_path>(?P<publish_path>/proj/(?P<project>[a0-z9_]+)/assets/(?P<asset_type>[a0-z9_]+)/(?P<asset_name>[a0-z9_]+)/cg/(?P<asset_step>[a0-z9]+)/publish)/v\d{3})[a0-z9_/.]+'

MANAGER_FACTORY = dict()


######################
# REGISTER CALLBACK
######################

def registerCallbacks(reg):
    event_filters = dict(
        Shotgun_PublishedFile_Change='sg_status_list'
    )

    arguments = dict(
        project_code_filter=['some_project', 'some_other_project']
    )

    reg.registerCallback(
        SCRIPT_NAME,
        SCRIPT_KEY,
        main,
        event_filters,
        arguments
    )


class Manager(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, entity, logger):
        super(Manager, self).__init__()
        self.published_file = entity
        self.logger = logger

    @classmethod
    def init_from_entity(cls, entity, logger):
        """

        :param entity: sg_dict with a published_file_type
        :return:
        """
        klass = MANAGER_FACTORY.get(entity.get('published_file_type', {}).get('name'))
        if klass:
            return klass(entity, logger)

    def validate(self):
        """
        An opportunity to validate before execution.
        :return: True by default
        """
        return True

    @abc.abstractmethod
    def execute(self):
        """Primary execution method."""

    def finalize(self):
        """An opportunity to do clean-up or any post-execution task."""

    def process(self):
        """Runs the execution logic"""
        try:
            if self.validate():
                self.execute()
                self.finalize()
        except Exception as e:
            self.logger.error(e.message)


class FBXManager(Manager):

    def __init__(self, entity, logger):
        super(FBXManager, self).__init__(entity, logger)
        self._publish_path = None
        self._version_path = None
        self._published_file_path = None

    @property
    def published_file_path(self):
        """Identifies the path of the published file on the SAN"""
        if not self._published_file_path:
            self._published_file_path = self.published_file.get('path', {}).get('local_path')
        return self._published_file_path

    @property
    def publish_path(self):
        """Identifies the context of the published file"""
        if not self._publish_path:
            self._publish_path = re.match(RE_FBX_PUBLISH_TEMPLATE, self.published_file_path).group('publish_path')
        return self._publish_path

    @property
    def version_path(self):
        """Identifies the context of the published file"""
        if not self._version_path:
            self._version_path = re.match(RE_FBX_PUBLISH_TEMPLATE, self.published_file_path).group('version_path')
        return self._version_path

    def validate(self):
        validate = super(FBXManager, self).validate()

        if not self.published_file_path:
            self.logger.error('Could not define the local path of %s', self.published_file.get('code'))
            validate = False

        if not re.match(RE_FBX_PUBLISH_TEMPLATE, self.published_file_path):
            self.logger.error('%s does not match the publish template', self.published_file_path)
            validate = False

        return validate

    def force_symlink(self):
        """Creates or overwrites current version symlink."""
        current_path = os.path.join(self.publish_path, 'current')
        cmd = ['/usr/bin/ln', '-sfn', self.version_path, current_path]
        subuprocess.Popen(cmd, stdout = sbuprocess.PIPE)


        if not os.path.abspath(os.path.realpath(current_path)) == os.path.abspath(os.path.realpath(self.version_path)):
            self.logger.error('The symlink %s does not point to %s', os.path.abspath(os.path.realpath(current_path)),
                              os.path.abspath(self.version_path))
        else:
            self.logger.info('linked %s -> %s', current_path, self.version_path)

    def execute(self):
        self.force_symlink()


def get_published_file(sg, event):
    return sg.find_one('PublishedFile', [['id', 'is', event['meta']['entity_id']]],
                       ['code', 'sg_status_list', 'path', 'published_file_type'])


MANAGER_FACTORY['Motion Builder FBX'] = FBXManager


def main(sg, logger, event, args):
    published_file = get_published_file(sg, event)
    if not published_file:
        logger.debug('Exiting, the published file couldn\'t be found.')
        return

    project = event['project']
    project = sg.find_one("Project", [['id', 'is', project['id']]], ['id', 'name', 'sg_project_code'])
    project_filter = args.get('project_code_filter')
    if project_filter and project['sg_project_code'] not in project_filter:
        return

    manager = Manager.init_from_entity(published_file, logger)
    if manager:
        manager.process()
    else:
        logger.debug('No manager implemented for PublishedFile: %s', published_file.get('code'))
