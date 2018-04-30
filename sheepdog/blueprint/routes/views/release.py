import flask
import uuid
from flask import request, current_app

from sheepdog import auth, models
from sheepdog.utils import parse
from sheepdog.errors import UserError


def create_release():
    """
    /admin/release/create
    """
    auth.admin_auth()

    # Check that there are no release candidates already
    with flask.current_app.db.session_scope():
        release_candidates = (current_app.db.nodes(models.DataRelease)
                                            .props(released=False).all())
        if len(release_candidates) != 0:
            raise UserError("Can not create release candidate. "
                            "At least one unreleased DataRelease node found")

    # Parse request data and check for errors
    release_props = ['major_version', 'minor_version']
    doc = parse.parse_request_json()

    error_msg = ("Provide a dictionary with {} keys "
                 "for release candidate node creation"
                 .format(release_props))

    if not isinstance(doc, dict):
        raise UserError(error_msg)

    if set(doc.keys()) != set(release_props):
        raise UserError(error_msg)

    major, minor = doc['major_version'], doc['minor_version']

    # Check against previous releases if release candidate version is valid
    assert_version_is_valid(major, minor)

    # Create release candidate node:
    with flask.current_app.db.session_scope() as session:
        session.add(
            models.DataRelease(
                node_id=str(uuid.uuid4()),
                major_version=major,
                minor_version=minor,
                released=False,
            )
        )
        msg = 'DataRelease node created: v{}.{}'.format(major, minor)

    return flask.jsonify({
        'message': msg
    })


def get_release():
    """
    /admin/release/get
    """
    auth.admin_auth()

    return_all = request.args.get('all', default=False, type=bool)

    with flask.current_app.db.session_scope():
        if return_all:
            response = current_app.db.nodes(models.DataRelease).all()
            response = {
                'release_history': sorted([r.to_json()['properties'] for r in response],
                                          key=lambda x: (x['major_version'], x['minor_version']),
                                          reverse=True)
            }
        else:
            response = current_app.db.nodes(models.DataRelease).props(released=False).all()

            # Throw error if there are more that one release candidate
            assert_no_multiple_release_candidates(response)

            if len(response) == 1:
                response = response[0].to_json()['properties']

    return flask.jsonify(response)


def set_release():
    """
    /admin/release/set
    """
    auth.admin_auth()

    doc = parse.parse_request_json()

    released = doc.get('released')
    release_date = doc.get('release_date')

    # Throw error if release date is earlier than previous release
    assert_release_date_is_valid(release_date)

    with flask.current_app.db.session_scope():
        response = current_app.db.nodes(models.DataRelease).props(released=False).all()

        if len(response) == 0:
            raise UserError("No release candidate found. Consider creating one")

        # Throw error if there are more that one release candidate
        assert_no_multiple_release_candidates(response)
        release = response[0]

        if release_date is None and released is None:
            raise UserError(
                "Nothing to set. "
                "Provide 'release_date' and/or 'released' parameters to set"
            )

        changes = {}
        for attribute in ['released', 'release_date']:
            value = locals()[attribute]
            if value is not None:
                changes[attribute] = {'old_value': getattr(release, attribute),
                                      'new_value': value}
                setattr(release, attribute, value)

        version_tag = 'v{}.{}'.format(release.major_version,
                                      release.minor_version)

    return flask.jsonify({
        'message': 'Succesfully updated release {}'.format(version_tag),
        'changes': changes,
    })


def assert_no_multiple_release_candidates(release_candidates):
    """
    Throw error if more then one release candidate
    """
    if len(release_candidates) > 1:
        raise Exception("Release nodes in bad state. Multiple release candidates: {}"
                        .format(release_candidates))


def assert_version_is_valid(major_version, minor_version):
    """
    Check against previous releases if release candidate version is valid:
    1. New version must be higher
    2. New major version can not be higher then last one +1
    """

    with flask.current_app.db.session_scope():
        previous_releases = (current_app.db.nodes(models.DataRelease)
                                           .props(released=True).all())
        max_major_version = max([r.major_version for r in previous_releases] or [0])
        max_minor_version = max([r.minor_version for r in previous_releases] or [0])

    msg = ("Last release version: {}.{}, Version provided: {}.{}"
           .format(max_major_version, max_minor_version,
                   major_version, minor_version))

    if major_version == max_major_version + 1:
        return

    elif major_version < max_major_version:
        msg = "Release version must be higher than last one. " + msg
        raise UserError(msg)

    elif major_version == max_major_version:
        if minor_version <= max_minor_version:
            msg = "Release version must be higher than last one. " + msg
            raise UserError(msg)

    else:
        msg = "Can not increase major version by more then one. " + msg
        raise UserError(msg)


def assert_release_date_is_valid(release_date):
    """
    Check against previous releases if release_date is valid:
    Must be None or later then last release date
    """
    if release_date is None:
        return

    with flask.current_app.db.session_scope():
        previous_releases = (current_app.db.nodes(models.DataRelease)
                                           .props(released=True).all())
        if previous_releases == []:
            return
        release_dates = sorted([r.release_date for r in previous_releases],
                               reverse=True)
        if release_dates[0] >= release_date:
            raise UserError(
                'Invalid release date. Provide date later than latest release ({})'
                .format(release_dates[0])
            )
