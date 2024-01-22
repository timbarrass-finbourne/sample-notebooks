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

    df = pd.DataFrame([c], columns=h)

    new_labels = pd.MultiIndex.from_arrays([df.columns, df.iloc[0]], names=['', ''])
    df = df.set_axis(new_labels, axis=1).iloc[1:]

    # Now build a row per breakdown
    for b in rule_result.rule_result.rule_breakdown:
        r = []
        for l in b.lineage:
            r.append(l.sub_label)
        r = r + [b.group_status,len(b.missing_data_information)]
        for k in sorted(b.results_used.keys()):
            r.append(b.results_used[k])

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