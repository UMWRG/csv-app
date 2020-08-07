import click
import json
import os
from hydra_client.connection import JSONConnection, RemoteJSONConnection
from hydra_client.click import hydra_app, make_plugins, write_plugins
from .importer import CSVImporter
from .exporter import CSVExporter


def get_client(hostname, session_id=None, **kwargs):
    if hostname is not None and hostname.startswith('http'):
        return RemoteJSONConnection(app_name='Hydra CSV App',
                                    url=hostname,
                                    session_id=session_id)
    else:
        return JSONConnection(app_name='Hydra CSV App',
                              db_url=hostname,
                              **kwargs)


def get_logged_in_client(context, user_id=None):
    session = context['session']
    client = get_client(context['hostname'], session_id=session, user_id=user_id)
    if client.user_id is None:
        client.login(username=context['username'], password=context['password'])
    return client


def start_cli():
    cli(obj={}, auto_envvar_prefix='HYDRA_CSV')


@click.group()
@click.pass_obj
@click.option('-u', '--username', type=str, default=None)
@click.option('-p', '--password', type=str, default=None)
@click.option('-h', '--hostname', type=str, default=None)
@click.option('-s', '--session', type=str, default=None)
def cli(obj, username, password, hostname, session):
    """ CLI for the Hydra CSV application. """

    obj['hostname'] = hostname
    obj['username'] = username
    obj['password'] = password
    obj['session'] = session


@hydra_app(category='import', name='Import CSV')
@cli.command(name='import', context_settings=dict(
    ignore_unknown_options=True,
    allow_extra_args=True))
@click.pass_obj
@click.option('--filename', '-f', type=click.Path(file_okay=True, dir_okay=False, exists=True))
@click.option('-p', '--project-id', type=int)
@click.option('--template-id', '-t', type=int)
@click.option('-u', '--user-id', type=int, default=None)
@click.option('-n', '--network-id', type=int)
@click.option('-s', '--scenario-name', type=int, default=None)
@click.option('--timezone', '-z', type=str, default=None)
@click.option('--ignore-filenames', default=False)
def import_csv(obj, filename, project_id, template_id, user_id,
               network_id, scenario_name, timezone, ignore_filenames,
               *args):

    """ Import a Hydra network from CSV files """
    click.echo(f'Beginning import of "{filename}"! Project ID: {project_id}')

    client = get_logged_in_client(obj, user_id=user_id)

    importer = CSVImporter(client, timezone=timezone)

    network_id, scenario_id = importer.import_data(filename, project_id, template_id,
                                                   network_id=network_id,
                                                   scenario_name=scenario_name,
                                                   ignore_filenames=ignore_filenames)

    click.echo(f'Successfully imported "{filename}"!'
               f' Network ID: {network_id}, Scenario ID: {scenario_id}')


@hydra_app(category='export', name='Export Hydra Network to CSV files')
@cli.command(name='export', context_settings=dict(
    ignore_unknown_options=True,
    allow_extra_args=True))
@click.pass_obj
@click.option('-d', '--data-dir', type=click.Path(exists=False), envvar='EXPORT_PATH', default=None)
@click.option('-n', '--network-id', type=int, default=None)
@click.option('-s', '--scenario-id', type=int, default=None)
@click.option('-u', '--user-id', type=int, default=None)
@click.option('--include-results', is_flag=True)
def export_csv(obj, data_dir, network_id, scenario_id, user_id, include_results):
    """ Export a Hydra to CSV files. """
    client = get_logged_in_client(obj, user_id=user_id)

    exporter = CSVExporter(client)

    if not data_dir or data_dir is None:
        data_dir = os.path.join('/tmp', str(network_id))
        try:
            os.mkdir(data_dir)
        except FileExistsError:
            pass

    if include_results:
        include_results = 'Y'
    else:
        include_results = 'N'

    exporter.export(network_id, scenario_id, data_dir, include_results)


    click.echo(f'Successfully exported Network ID: {network_id}, Scenario ID: {scenario_id}')

@hydra_app(category='import-template', name='Import Template')
@cli.command(name='import-template', context_settings=dict(
    ignore_unknown_options=True,
    allow_extra_args=True))
@click.pass_obj
@click.option('--filename', '-f', type=click.Path(file_okay=True, dir_okay=False, exists=True))
@click.option('-u', '--user-id', type=int, default=None)
def import_template(obj, filename, user_id, *args):

    """ Import a Hydra network from CSV files """
    click.echo(f'Beginning import of "{filename}"!')

    client = get_logged_in_client(obj, user_id=user_id)

    with open(filename, 'r') as xmlfile:
        content = '\n'.join(xmlfile.readlines())
        template = client.import_template_xml(content)

    click.echo(f'Successfully imported "{filename}"! Template ID: {template.id}')
