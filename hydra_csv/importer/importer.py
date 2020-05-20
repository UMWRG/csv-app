def import():
    parser = commandline_parser()
    args = parser.parse_args()
    csv = ImportCSV(url=args.server_url, session_id=args.session_id)

    network_id = None
    scen_ids = []
    errors = []
    try:

        write_progress(1,csv.num_steps)
        validate_plugin_xml(os.path.join(__location__, 'plugin.xml'))

        if args.expand_filenames:
            csv.expand_filenames = True

        if args.timezone is not None:
            csv.timezone = pytz.timezone(args.timezone)

        # Create project and network only when there is actual data to
        # import.
        write_progress(2,csv.num_steps)
        csv.create_project(ID=args.project, network_id=args.network_id)
        csv.create_scenario(name=args.scenario)
        csv.create_network(file=args.network, network_id=args.network_id)

        write_progress(3,csv.num_steps)
        for nodefile in csv.node_args:
            write_output("Reading Node file %s" % nodefile)
            csv.read_nodes(nodefile)
            log.info("Finished reading nodes")

        write_progress(4,csv.num_steps)
        if len(csv.link_args) > 0:
            for linkfile in csv.link_args:
                write_output("Reading Link file %s" % linkfile)
                csv.read_links(linkfile)
                log.info("Finished reading links")
        else:
            log.warn("No link files found")
            csv.warnings.append("No link files found")

        write_progress(5,csv.num_steps)
        if len(csv.group_args) > 0:
            for groupfile in csv.group_args:
                write_output("Reading Group file %s"% groupfile)
                csv.read_groups(groupfile)
                log.info("Finished reading groups")
        else:
            log.warn("No group files specified.")
            csv.warnings.append("No group files specified.")

        write_progress(6,csv.num_steps)
        if len(csv.groupmember_args) > 0:
            write_output("Reading Group Members")
            for groupmemberfile in csv.groupmember_args:
                csv.read_group_members(groupmemberfile)
        else:
            log.warn("No group member files specified.")
            csv.warnings.append("No group member files specified.")

        write_progress(7,csv.num_steps)
        write_output("Saving network")
        csv.commit()
        if csv.NetworkSummary.get('scenarios') is not None:
            scen_ids = [s['id'] for s in csv.NetworkSummary['scenarios']]

        write_progress(9,csv.num_steps)
        if len(csv.rule_args) > 0 and csv.rule_args[0] != "":
            write_output("Reading Rules")
            for s in csv.NetworkSummary.get('scenarios'):
                if s.name == csv.Scenario['name']:
                    scenario_id = s.id
                    break

            rule_reader = RuleReader(csv.connection, scenario_id, csv.NetworkSummary, csv.rule_args)

            rule_reader.read_rules()

        network_id = csv.NetworkSummary['id']

        write_progress(9,csv.num_steps)
        write_output("Saving types")
        if args.template is None:
            raise HydraPluginError("No template specified. Please specify a template ID")
        else:
            csv.template_id = args.template
            try:
                warnings = csv.set_resource_types()
                csv.warnings.extend(warnings)
            except Exception as e:
                raise HydraPluginError("An error occurred setting the types from the template. "
                                       "Error relates to \"%s\" "
                                       "Please check the template and resource types."%(e.message))
        write_progress(9,csv.num_steps)

    except HydraPluginError as e:
        if len(errors) == 0:
            errors = [e.message]
        log.exception(e)
    except Exception as e:
        log.exception(e)
        errors = [e]

    xml_response = create_xml_response('ImportCSV',
                                       network_id,
                                       scen_ids,
                                       errors,
                                       csv.warnings,
                                       csv.message,
                                       csv.files)

    print(xml_response)
