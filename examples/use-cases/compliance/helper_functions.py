import pandas as pd
import datetime
from datetime import datetime, timedelta, time, date
import pytz
import lusid.models as lm

def delete_all_current_instruments(instruments_api):
    response = instruments_api.list_instruments()
    if len(response.values) == 0:
        print('No previous existing instruments')
        return None
    for ii in range(len(response.values)):
        identifierTypes=[]
        identifier=[]
        IDs = response.values[ii].identifiers.keys()
        for key in IDs:
            identifierTypes.append(key)
            identifier.append(response.values[ii].identifiers[key])
        print(identifierTypes)
        print(identifier)
        deleted = instruments_api.delete_instrument(identifierTypes[0], identifier[0])
        print(deleted)

def delete_all_current_portfolios(portfolios_api, scope):
    response = portfolios_api.list_portfolios(filter=f"id.scope eq '{scope}'")
    if len(response.values) == 0:
        print('No previous existing portfolios')
        return None
    for ii in range(len(response.values)):
        code = response.values[ii].id.code
        scope = response.values[ii].id.scope 
        print('Deleting:')
        print('Code: {} \nScope: {}'.format(code, scope))
        portfolios_api.delete_portfolio(scope, code)
        print('All scopes deleted')

def create_portfolio_group(portfolio_groups_api, scope, code, portfolios):

    portfolio_creation_date = datetime.now(pytz.UTC) - timedelta(days=5000)

    try:
        portfolio_groups_api.delete_portfolio_group(
            scope=scope,
            code=code)
    except:
        pass
    
    group_request = lm.CreatePortfolioGroupRequest(
        code=code,
        display_name=code,
        values=portfolios,
        sub_groups=None,
        description=None,
        created=portfolio_creation_date)

    portfolio_group = portfolio_groups_api.create_portfolio_group(
        scope=scope,
        create_portfolio_group_request=group_request)
    
    return portfolio_group

def aggregation_request(valuation_effectiveAt, price_field, scope):
    inline_recipe = lm.ConfigurationRecipe(
        scope="User",
        code="valuation_recipe",
        market=lm.MarketContext(
            market_rules=[
                # define how to resolve the quotes
                lm.MarketDataKeyRule(
                    key="Quote.Isin.*",
                    supplier="Lusid",
                    data_scope=scope,
                    quote_type="Price",
                    field=price_field,
                ),
            ],
            options=lm.MarketOptions(
                default_supplier="Lusid",
                default_instrument_code_type="Isin",
                default_scope=scope,
            ),
        ),
        pricing=lm.PricingContext(
            options={"AllowPartiallySuccessfulEvaluation": True},
        ),
    )

    return lm.AggregationRequest(
        inline_recipe=inline_recipe,
        metrics=[
            lm.AggregateSpec("Instrument/default/Name", "Value"),
            lm.AggregateSpec("Valuation/PvInReportCcy", "Proportion"),
            lm.AggregateSpec("Valuation/PvInReportCcy", "Sum"),
            lm.AggregateSpec("Holding/default/Units", "Sum"),
            lm.AggregateSpec("Aggregation/Errors", "Value"),
        ],
        group_by=["Instrument/default/Name"],
        # choose the valuation time for the request
        effective_at=valuation_effectiveAt,
    )

def generate_valuation_request(valuation_effectiveAt, price_field, scope, portfolio_code):

    # Create the valuation request
    valuation_request = lm.ValuationRequest(
        recipe_id=lm.ResourceId(
            scope="User", code="valuation_recipe" + "_" + price_field
        ),
        metrics=[
            lm.AggregateSpec("Instrument/default/Name", "Value"),
            lm.AggregateSpec("Valuation/PvInReportCcy", "Proportion"),
            lm.AggregateSpec("Valuation/PvInReportCcy", "Sum"),
            lm.AggregateSpec("Holding/default/Units", "Sum"),
            lm.AggregateSpec("Aggregation/Errors", "Value"),
        ],
        group_by=["Instrument/default/Name"],
        portfolio_entity_ids=[
            lm.PortfolioEntityId(scope=scope, code=portfolio_code)
        ],
        valuation_schedule=lm.ValuationSchedule(
            effective_at=valuation_effectiveAt.isoformat()
        ),
    )

    return valuation_request

def create_property_definition(properties_api, domain, scope, code, data_type):
    properties_api.create_property_definition(
        create_property_definition_request=lm.CreatePropertyDefinitionRequest(
            domain=domain,
            scope=scope,
            code=code,
            display_name=code,
            life_time="Perpetual",
            value_required=False,
            data_type_id=lm.resource_id.ResourceId(scope="system", code=data_type)
        )
    )

def rule_level_dataframe(run_summary):
    # Use the first result as a way of generating overall headers
    h = ['', '', '', '', '']
    c = ['Rule', 'Rule Description', 'Status', 'Affected Orders', 'Affected Portfolios']

    df = pd.DataFrame([c], columns=h)

    new_labels = pd.MultiIndex.from_arrays([df.columns, df.iloc[0]], names=['', ''])
    df = df.set_axis(new_labels, axis=1).iloc[1:]

    # Now build a row per result
    for d in run_summary.details:
        r = [f"{d.rule_id.scope}/{d.rule_id.code}", d.rule_description, d.status, len(d.affected_orders), len(d.affected_portfolios_details)]

        df.loc[len(df)] = r

    return df

def rule_result_dataframe(rule_result):
    # Use the first breakdown as a way of generating overall headers
    h = []
    c = []
    for l in rule_result.rule_result.rule_breakdown[0].lineage:
        h.append('Lineage')
        c.append(l.label)
    h = h + ['Details','Details']
    c = c + ['Status','Missing Data']
    for r in sorted(rule_result.rule_result.rule_breakdown[0].results_used.keys()):
        h.append('Results Used')
        c.append(r)
    for r in sorted(rule_result.rule_result.parameters_used.keys()):
        h.append('Parameters Used')
        c.append(r)

    df = pd.DataFrame([c], columns=h)

    new_labels = pd.MultiIndex.from_arrays([df.columns, df.iloc[0]], names=['', ''])
    df = df.set_axis(new_labels, axis=1).iloc[1:]

    p = []
    for k in sorted(rule_result.rule_result.parameters_used.keys()):
        p.append(rule_result.rule_result.parameters_used[k])

    # Now build a row per breakdown
    for b in rule_result.rule_result.rule_breakdown:
        r = []
        for l in b.lineage:
            r.append(l.sub_label)
        r = r + [b.group_status,len(b.missing_data_information)]
        for k in sorted(b.results_used.keys()):
            r.append(b.results_used[k])
        r = r + p

        df.loc[len(df)] = r

    return df

def decimal_parameter(val):
    return lm.DecimalComplianceParameter(value=str(val),compliance_parameter_type='DecimalComplianceParameter')

def propertykey_parameter(key):
    return lm.PropertyKeyComplianceParameter(value=key,compliance_parameter_type='PropertyKeyComplianceParameter')

def stringlist_parameter(scope, code):
    return lm.StringListComplianceParameter(value=lm.ResourceId(scope=scope, code=code),compliance_parameter_type='StringListComplianceParameter')

def portfolioidlist_parameter(scope, code):
    return lm.PortfolioIdListComplianceParameter(value=lm.ResourceId(scope=scope,code=code),compliance_parameter_type='PortfolioIdListComplianceParameter')

def experimental_plot_lineage(rule_result):
    import networkx as nx
    from collections import defaultdict
    import igraph as ig
    import matplotlib.pyplot as plt
    import pprint

    pp = pprint.PrettyPrinter()

    graph = defaultdict(set)
    graph['Initial'] = set()
    layers = dict()
    layers['Initial'] = 0
    for b in rule_result.rule_result.rule_breakdown:
        last = "Initial"
        layer = 1
        for l in b.lineage[1:-1]:
            graph[last].add(l.sub_label)
            layers[l.sub_label] = layer
            last = l.sub_label
            layer = layer + 1

    edges = []
    vertices = set()
    for n in graph.keys():
        vertices.add(n); 
        for d in graph[n]:
            edges.append((n,d))
            vertices.add(d)
    vertices = [v for v in vertices]
    layers = [layers[v] for v in vertices]

    I = ig.Graph(directed=True)
    I.add_vertices(vertices)
    I.add_edges(edges)
    layout = I.layout_reingold_tilford(root=[0])

    I.vs["label"] = vertices
    visual_style = {
        "edge_width": 0.3,
        "vertex_width": 100,
        "palette": "heat",
        "vertex_shape": "rectangle",
        "vertex_label_size": 8
    }

    fig, ax = plt.subplots()
    ax.invert_yaxis()
    ig.plot(I, layout=layout, target=ax, **visual_style)

def variation_steps_to_dataframe(s):
    h = []
    c = []
    c.append("Step Label"); c.append("Step Type")
    h = h + ["",""]

    if (s.compliance_step_type == "GroupFilterStep"):
        for p in sorted(s.limit_check_parameters, key=lambda x: x.name):
            h.append("Limit Params")
            c.append(p.name)
        for p in sorted(s.warning_check_parameters, key=lambda x: x.name):
            h.append("Warning Params")
            c.append(p.name)
    else:
        for p in sorted(s.parameters, key=lambda x: x.name):
            h.append("Params")
            c.append(p.name)

    df = pd.DataFrame([c], columns=h)

    new_labels = pd.MultiIndex.from_arrays([df.columns, df.iloc[0]], names=['', ''])
    df = df.set_axis(new_labels, axis=1).iloc[1:]

    r = []
    r.append(s.label); r.append(s.compliance_step_type)

    if (s.compliance_step_type == "GroupFilterStep"):
        for p in sorted(s.limit_check_parameters, key=lambda x: x.name):
            r.append(p.type)
        for p in sorted(s.warning_check_parameters, key=lambda x: x.name):
            r.append(p.type)
    else:
        for p in sorted(s.parameters, key=lambda x: x.name):
            r.append(p.type)

    df.loc[len(df)] = r

    return df