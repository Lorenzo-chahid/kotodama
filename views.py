from django.shortcuts import render, redirect
from django.http import HttpResponseRedirect
from django.core import serializers
from django.db.models import Q
from django.http import HttpResponse, Http404, JsonResponse
from django.core.mail import send_mail
from app.models import Category, Portfolio, Client, Transaction, Valuation, ClientAsset, User, Expense, \
Forex, PrincipalsSerializer, PrincipalSummarySerializer, WatchList, MatchClientsPortfolios,\
UserSerializer, File, Report, Property, Activity, Filetype, UserType, UserPermission, Permission
from django.contrib.auth.models import User as auth_User
from django.contrib.auth.tokens import default_token_generator  
from app.decorators import user_passes_test
from functools import wraps
from logs.models import Logs
from rest_framework.renderers import JSONRenderer
import simplejson as json
import pymysql, numpy as np, datetime
from dateutil.relativedelta import *
import sys
sys.path.insert(1, '../')
## custom scripts TODO make modules
import app.app_custom as app
import connectDB as c
from django.contrib.auth import authenticate, login
import app.draastic3 as d
import app.report as report
from django.forms.models import model_to_dict
from datetime import datetime as DT
from pandas.tseries.offsets import BDay
import app.stress as stress
import os
import pandas as pd
from app.draastic3 import get_histo_prices
import pymysql
import random
import string
import requests

sys.path.insert(1, '../')
## LOGIN 


def test_login(request):
    user = authenticate(username='yves.coignard@gmail.com', password='arcole')
    login(request, user)
    return HttpResponse('logged in ', content_type='application/json')


def is_logged_in(request):
    return HttpResponse(not request.user.is_anonymous)


def index(request):
    if not request.user.is_anonymous:
        return render(request, "index.html")
    else:
        return render(request, "index.html")


@user_passes_test()
def getUser(request):
    result = serializers.serialize('json', [request.user.user])
    user_name = json.dumps({"username": request.user.username})
    result = json.loads(result)
    result[0]['fields']['username'] = request.user.username
    result = json.dumps(result)
    return HttpResponse(result, content_type='application/json')


@user_passes_test()
def getPortfolios(request):
    match = MatchClientsPortfolios.objects.select_related('client').filter(client__user=request.user.id)
    portfolio_list = [x.portfolio_id for x in match]
    portfolios = Portfolio.objects.filter(pk__in=portfolio_list)
    result = []
    for portfolio in portfolios:
        valuations = app.get_valuations_v2([portfolio.pk])
        if valuations['values']:
            result.append({
                "name": portfolio.name, 
                "currency": portfolio.currency_id, 
                "valuation": valuations, 
                "principal": portfolio.client.name, 
                "id": portfolio.id, 
                "principal_id": portfolio.client.id
            })
    result = json.dumps(result)
    return HttpResponse(result, content_type='application/json')


@user_passes_test()
def getPortfolioStatistics(request):
    portfolio_id = request.GET['portfolio']
    portfolio = Portfolio.objects.get(pk=portfolio_id)
    if portfolio.client.user_id != request.user.id:
        return HttpResponse('Unauthorized access', content_type='application/json')
    d_request = 'risk'
    [portfolio, baseCurrency, FX] = d.readPortfolio([portfolio_id])
    [portfolioValue, ref_alloc, ac, pePtf, liquidAssetClasses, peAssetClasses, weightPE, fxExposure] = d.ptf2ac(portfolio, FX)
    risk = d.main(baseCurrency, ref_alloc, liquidAssetClasses, pePtf, portfolioValue, weightPE, fxExposure, 0, d_request)
    perf = app.compute_time_performance(portfolio_id)
    perf["Information"] = (float(risk['expectedReturn']) - float(perf["Information"])) / float(risk['vol'])
    result = {"risk": risk, "performance": perf}
    result = json.dumps(result)
    return HttpResponse(result, content_type='application/json')


def delete_portfolio(request):
    response = JsonResponse({"response": False}, status=400)
    if request.method == "POST":
        id = json.loads(request.POST.get("id"))
        Portfolio.objects.filter(pk=id).delete()
        response = JsonResponse({"response": True}, status=200)
    return response


@user_passes_test()
def getInvestors(request):
    portfolio_id = request.GET['portfolio']
    portfolio = Portfolio.objects.get(pk=portfolio_id)
    if portfolio.client.user_id != request.user.id:
        return HttpResponse('Unauthorized access', content_type='application/json')
    result = []
    matches = MatchClientsPortfolios.objects.filter(portfolio_id=portfolio_id).select_related('client').all()
    if portfolio.type == 'fund':
        for match in matches:
            valuation = Valuation.objects.filter(portfolio_id=match.portfolio_id).all()
            purchase_date = max(match.purchaseDate, valuation[0].dateval).strftime('%Y-%m-%d')
            last_price = valuation.last().valuation
            value = match.nbShares * last_price
            unrealized_gl = match.nbShares * (last_price - match.purchasePrice)
            result.append({
                "name": match.client.name, 
                "purchaseDate": purchase_date, 
                "purchasePrice": match.purchasePrice,
                "nbShares": match.nbShares, 
                "lastPrice": last_price, 
                "value": value, 
                "unrealizedGL": unrealized_gl
            })
    else:
        match = matches[0]
        valuation = Valuation.objects.filter(portfolio_id=match.portfolio_id).latest('dateval')
        result.append({
            "name": match.client.name, 
            "purchaseDate": '', 
            "purchasePrice": '-', 
            "nbShares": '-', 
            "lastPrice": valuation.valuation, 
            "value": valuation.valuation, 
            "unrealizedGL": '-'
        })
    result = json.dumps(result)
    return HttpResponse(result, content_type='application/json')


def getFXSpot(cur1, cur2):
    result = 1
    if cur1 != cur2:
        value1 = Forex.objects.get(pk=cur1).value if cur1 != 'GBP' else 1
        value2 = Forex.objects.get(pk=cur2).value if cur2 != 'GBP' else 1
        result = value2 / value1
    return(result)


@user_passes_test()
def getHoldings(request):
    beginning_of_month = datetime.date.today().replace(day=1)
    end_of_month = beginning_of_month + relativedelta(months=1) - datetime.timedelta(days=1)
    if 'portfolio' in request.GET:
        level_id = request.GET['portfolio']
        level = 'portfolio'
    else:
        level = request.GET['level']
        level_id = request.GET['level_id']
    # set fxPortfolio and currency to default values .. just to skip 'request' object in the next func call
    fxPortfolio = request.user.user.baseCurrency.value
    currency = request.user.user.baseCurrency.currency
    request_user_id = request.user.id
    holdings = calc_holdings(request_user_id, level, level_id, fxPortfolio, currency)
    result = json.dumps(holdings)
    return HttpResponse(result, content_type='application/json')


def calc_holdings(request_user_id, level, level_id, fxPortfolio=None, currency=None):
    beginning_of_month = datetime.date.today().replace(day=1)
    end_of_month = beginning_of_month + relativedelta(months=1) - datetime.timedelta(days=1)
    ## CHECK that portfolio belongs to user
    # print(level)
    if level == 'principal':
        query = Transaction.objects.select_related('asset', 'portfolio').filter(portfolio__client_id=level_id).order_by('asset').all()
        fxPortfolio = Client.objects.get(id=level_id).baseCurrency.value
        currency = Client.objects.get(id=level_id).baseCurrency.currency
    elif level == 'portfolio':
        query = Transaction.objects.select_related('asset', 'portfolio').filter(portfolio_id=level_id).order_by('asset').all()
        fxPortfolio = Portfolio.objects.select_related('currency').get(id=level_id).currency.value
        currency = Portfolio.objects.select_related('currency').get(id=level_id).currency.currency
    elif level == 'family':
        query = Transaction.objects.select_related('asset', 'portfolio').filter(portfolio__client__user_id=request_user_id).filter(portfolio__client__family=level_id).order_by('asset').all()
    else:
        return []
    holdings = []
    holding = {}
    previousId = None
    couponAmort = 0
    rentAmort = 0
    loanAmort = 0
    portfolioCurrency = currency
    for item in query:
        couponAmort += item.asset.couponAmort * item.nbShares * fxPortfolio / item.asset.currency.value  ## Coupon amortization for bonds
        fx = getFXSpot(item.asset.currency.currency, portfolioCurrency)
        if item.asset.id == previousId: ## Aggregate positions
            # avgPurchasePrice = (holding['price'] * holding['nbShares'] + item.asset.price * item.nbShares) / (holding['nbShares'] + item.nbShares) ## Average Price TODO FIFO, LIFO
            purchasePriceNumerator += item.asset.price * item.nbShares
            holding['nbShares'] += item.nbShares
            holding['unrealizedGL'] += item.nbShares * (item.asset.price - item.price) * fx
            holding['value'] += item.asset.price * item.nbShares
            holding['valueInPortfolioCurrency'] = holding['fx'] * holding['value']
        else:
            if holding != {}:

                key_order = ['asset', 'isin', 'name', 'nbShares', 'price', 'value', 'currency', 'fx',
                            'portfolioCurrency','valueInPortfolioCurrency', 'unrealizedGL',
                            'category','category_short']
                holding = {key:holding[key] for key in key_order if key in holding.keys()}

                holdings.append(holding)
            holding = {}
            ppty = {}
            purchasePriceNumerator = item.asset.price * item.nbShares
            if item.asset.type == 'fx':
                holding['price'] = item.asset.currency.value
                holding['value'] = (item.asset.currency.value - item.price) * item.nbShares## pricing of FX forward
                holding['portfolioCurrency'] = Portfolio.objects.select_related('currency').get(id=level_id).currency.currency
            else:
                holding['price'] = 1 if item.asset.type == 'cash' else item.asset.price
                holding['value'] = item.nbShares if item.asset.type == 'cash' else item.asset.price * item.nbShares
                holding['portfolioCurrency'] = Portfolio.objects.select_related('currency').get(id=item.portfolio.id).currency.currency
            if item.asset.type == 'property':
                if 'value' not in holding.keys():
                    holding['value'] = 0
                properties = Property.objects.filter(asset_id=item.asset.id)
                accrued_rent = 0
                accrued_loan = 0
                for p in properties:
                    accrued_rent += p.monthly_rent * (datetime.date.today() - beginning_of_month).days / (end_of_month - beginning_of_month).days
                    accrued_loan -= p.loan_rate * p.loan_amount / 12 * (datetime.date.today() - beginning_of_month).days / (end_of_month - beginning_of_month).days
                    holding['loan_amount'] = p.loan_amount
                    holding['loan_rate'] = p.loan_rate
                holding["accrued_loan"] = accrued_loan
                holding["accrued_rent"] = accrued_rent
            holding['asset'] = item.asset_id
            holding['name'] = item.asset.name
            holding['isin'] = item.asset.isin
            holding['currency'] = item.asset.currency.currency
            holding['nbShares'] = item.nbShares
            holding['category'] = item.asset.category.name3
            holding['category_short'] = item.asset.category.level1
            holding['unrealizedGL'] = item.nbShares * (item.asset.price - item.price) * fx
            holding['portfolioCurrency'] = Portfolio.objects.select_related('currency').get(id=item.portfolio.id).currency.currency
            previousId = item.asset.id
            if item.asset.type == 'bond':
                holding['price'] *= 100 ## Bond display prices in pct
            if item.asset.type == 'cash':
                holding['unrealizedGL'] = 0
            holding['fx'] = fx
            if level == 'portfolio':
                holding['valueInPortfolioCurrency'] = holding['fx'] * holding['value']
    if holding != {}:
        key_order = ['asset', 'isin', 'name', 'nbShares', 'price', 'value', 'currency', 'fx',
                     'portfolioCurrency', 'valueInPortfolioCurrency', 'unrealizedGL',
                     'category', 'category_short']
        holding = {key: holding[key] for key in key_order if key in holding.keys()}
        holdings.append(holding)
    if couponAmort > 0:
        holdings.append({
            "name": "coupon amortization",
            "isin": "",
            "price": 1,
            "nbShares": couponAmort,
            "currency": currency,
            "value": couponAmort,
            "unrealizedGL": 0,
            "category": 'mm' + currency.lower(),
            "portfolioCurrency": portfolioCurrency
        })
    # if level == 'portfolio' and holding != {}:
    #     holding['fx'] = getFXSpot(holding['currency'], holding['portfolioCurrency'])
    #     holding['valueInPortfolioCurrency'] = holding['fx'] * holding['value']

    return holdings

def csv_safe_check(rec):
    # make a string csv-readable if it contains comma
    # convert object to string => check if it contains comma =>
    # replace double quotes with double double quotes, add double quotes in the beginning and in the end of the string
    delimiter = ","
    if not isinstance(rec, str):
        rec = str(rec)
    # remove '#' is it would break csv generation by vuejs

    if rec.find(delimiter) >= 0:
        rec = rec.replace("\"", "\"\"")
        rec = "\"" + rec + "\""
    rec = rec.replace('#', '')

    return rec

@user_passes_test()
def get_transactions(request):
    request_user_id = request.user.id
    csv_safe_flag = True

    if 'portfolio' in request.GET:
        level_id = request.GET.get('portfolio', None)
        level = 'portfolio'
    else:
        level = request.GET.get('level', None)
        level_id = request.GET.get('level_id', None)

    if not level_id or not level:
        return JsonResponse({})

    # check permissions
    if level == 'principal':
        if Client.objects.get(pk=level_id).user_id != request_user_id:
            return HttpResponse('Unauthorized access', content_type='application/json')
    elif level == 'portfolio':
        if Portfolio.objects.select_related('client').get(pk=level_id).client.user_id != request_user_id:
            return HttpResponse('Unauthorized access', content_type='application/json')
    elif level == 'family':
        pass
    else:
        return HttpResponse('unknown level', content_type='application/json')

    holdings = []
    # currency = None
    # query = None
    couponAmort = 0

    # load transactions and currency
    if level == 'principal':
        query = Transaction.objects.select_related('asset', 'portfolio').filter(portfolio__client_id=level_id).order_by('asset').all()
        currency = Client.objects.get(id=level_id).baseCurrency.currency
    elif level == 'portfolio':
        query = Transaction.objects.select_related('asset', 'portfolio').filter(portfolio_id=level_id).order_by('asset').all()
        currency = Portfolio.objects.select_related('currency').get(id=level_id).currency.currency
    elif level == 'family':
        query = Transaction.objects.select_related('asset', 'portfolio').\
                filter(portfolio__client__user_id=request_user_id).\
                filter(portfolio__client__family=level_id).order_by('asset').all()
        currency = request.user.user.baseCurrency.currency
    else:
        return JsonResponse({})

    # calc 'value' of each transaction
    beginning_of_month = datetime.date.today().replace(day=1)
    end_of_month = beginning_of_month + relativedelta(months=1) - datetime.timedelta(days=1)
    for item in query:
        # fx_rate - fx rate between principal/family/portfolio currency and asset currency
        fx_rate = getFXSpot(item.asset.currency.currency, currency)
        ptf_currency = Portfolio.objects.select_related('currency').get(id=item.portfolio.id).currency.currency

        # ptf_fx_rate - portfolio fx rate between portfolio currency and asset currency
        ptf_fx_rate = getFXSpot(item.asset.currency.currency, ptf_currency)

        holding = {
            'asset': item.asset_id,
            'name': item.asset.name,
            'isin': item.asset.isin,
            'currency': item.asset.currency.currency,
            'nbShares': item.nbShares,
            'category': item.asset.category.name3,
            'category_short': item.asset.category.level1,
            'unrealizedGL': item.nbShares * (item.asset.price - item.price) * ptf_fx_rate,
            'purchaseDate': item.date.strftime('%Y-%m-%d'),
            'status': item.status
        }

        if item.asset.type == 'fx':
            holding['price'] = item.asset.currency.value
            holding['value'] = (item.asset.currency.value - item.price) * item.nbShares  ## pricing of FX forward
        elif item.asset.type == 'cash':
            holding['price'] = 1
            holding['value'] = item.nbShares
        elif item.asset.type == 'property':
            holding['price'] = item.asset.price
            holding['value'] = item.asset.price * item.nbShares
            properties = Property.objects.filter(asset_id=item.asset.id)
            accrued_rent = 0
            accrued_loan = 0
            for p in properties:
                accrued_rent += p.monthly_rent * (datetime.date.today() - beginning_of_month).days / (
                            end_of_month - beginning_of_month).days
                accrued_loan -= p.loan_rate * p.loan_amount / 12 * (datetime.date.today() - beginning_of_month).days / (
                            end_of_month - beginning_of_month).days
                holding['loan_amount'] = p.loan_amount
                holding['loan_rate'] = p.loan_rate
            holding["accrued_loan"] = accrued_loan
            holding["accrued_rent"] = accrued_rent
        elif item.asset.type == 'bond':
            # Bond display prices in pct
            holding['price'] = item.asset.price
            holding['value'] = item.asset.price * item.nbShares
        else:
            holding['price'] = item.asset.price
            holding['value'] = item.asset.price * item.nbShares

        # Coupon amortization for bonds using fx rate depending on the 'level' choice
        couponAmort += item.asset.couponAmort * item.nbShares * fx_rate

        holding['portfolioCurrency'] = ptf_currency
        holding['fx'] = ptf_fx_rate
        holding['valueInPortfolioCurrency'] = ptf_fx_rate * holding['value']
        holding['unrealizedGL'] = item.nbShares * (item.asset.price - item.price) * ptf_fx_rate

        if item.asset.type == 'cash':
            holding['unrealizedGL'] = 0

        if csv_safe_flag:
            holding['name'] = csv_safe_check(holding['name'])
            holding['isin'] = csv_safe_check(holding['isin'])

        # add transaction to the list
        key_order = ['asset', 'isin', 'name', 'nbShares', 'price', 'value', 'currency', 'purchaseDate', 'status', 'fx',
                     'portfolioCurrency', 'valueInPortfolioCurrency', 'unrealizedGL', 'category']
        holding_sorted = {key: holding[key] for key in key_order if key in holding.keys()}
        # add all fields which are not sorted as per line above
        holding_sorted.update({key: holding[key] for key in holding.keys() if key not in key_order})
        holdings.append(holding_sorted)

    # add coupon amortisation
    if couponAmort > 0:
        holdings.append({
            "name": "coupon amortization",
            "isin": "",
            "price": 1,
            "nbShares": couponAmort,
            "currency": currency,
            "value": couponAmort,
            "unrealizedGL": 0,
            "category": 'mm' + currency.lower(),
            "portfolioCurrency": currency
        })
    return JsonResponse(holdings, safe=False)


@user_passes_test()
def portfolio_valuation(request_user_id, portfolio_id):
    # portfolio_id = int(request.GET['portfolio_id']) if 'portfolio_id' in request.GET else None
    # request_user_id = request.user.user.id
    if not portfolio_id:
        return False
        # return JsonResponse([], safe=False)
    holdings = calc_holdings(request_user_id, 'portfolio', portfolio_id)
    portfolio_value = sum([x['valueInPortfolioCurrency'] for x in holdings])

    Valuation.objects.filter(portfolio_id=portfolio_id, dateval=DT.today()).delete()
    Valuation.objects.create(
        valuation=portfolio_value,
        netinflows=portfolio_value,
        dateval=DT.today(),
        portfolio_id=portfolio_id)
    # return JsonResponse(holdings, safe=False)
    return True


@user_passes_test()
def getPrincipals(request):
    user = request.user.id
    principals = Client.objects.select_related('user').filter(user=user)
    serializer = PrincipalsSerializer(principals, many=True)
    result = JSONRenderer().render(serializer.data)
    return HttpResponse(result, content_type='application/json')


def delete_principals(request):
    response = JsonResponse({"response": False}, status=400)
    id = json.loads(request.POST.get("id"))
    principal = Client.objects.filter(id=id)
    principal.delete()
    response = JsonResponse({"response": True}, status=200)
    return response


@user_passes_test()
def getPrincipalDetail(request):
    principal = request.GET['principal']
    client = Client.objects.get(pk=principal)    
    if client.user_id != request.user.id:
        return HttpResponse('Unauthorized access', content_type='application/json')
    cashflows = Expense.objects.filter(client_id=principal).all()
    cashflows = [model_to_dict(x) for x in cashflows]
    # match = MatchClientsPortfolios.objects.select_related('client').filter(client_id=principal)
    # portfolio_list = [x.portfolio_id for x in match]
    # portfolios = Portfolio.objects.filter(pk__in=portfolio_list)
    # summary = app.compute_summary(portfolios)
    summary = app.summary_principal(client)
    result = {"cashflows": cashflows, "summary": summary, "detail": model_to_dict(client)}
    result = json.dumps(result, ensure_ascii=False).encode('utf8')
    return HttpResponse(result, content_type='application/json')


@user_passes_test()
def getAccountSummary(request):
    user_id = request.user.id
    level = request.GET['level']
    if level not in ['account', 'account_detailed']:
        level_id = request.GET['level_id']
    if level == 'account':
        summary = app.summary_account(user_id)
    elif level == 'account_detailed':
        summary = app.summary_account_detailed(user_id)
    elif level == 'principal':
        if Client.objects.get(pk=level_id).user_id != request.user.id:
            return HttpResponse('Unauthorized access', content_type='application/json')
        principal = Client.objects.get(id=level_id)
        summary = app.summary_principal(principal)
    elif level == 'portfolio':
        if Portfolio.objects.get(pk=level_id).client.user_id != request.user.id:
            return HttpResponse('Unauthorized access', content_type='application/json')
        portfolio_obj = Portfolio.objects.select_related('client').get(id=level_id)
        summary = app.summary_portfolio(portfolio_obj)
    result = json.dumps(summary, ensure_ascii=False).encode('utf8')
    return HttpResponse(result, content_type='application/json')


@user_passes_test()
def getValuations(request):
    user = request.user.id
    level = request.GET['level']
    aggregate = True
    if level not in ('account', 'list'):
        level_id = request.GET['level_id']
    if level in ('account', 'list'):
        portfolios = Portfolio.objects.select_related('client').filter(client__user=user).values_list('pk')
    elif level == 'principal':
        portfolios = Portfolio.objects.select_related('client').filter(client_id=level_id).values_list('pk')
        if Client.objects.get(pk=level_id).user_id != request.user.id:
            return HttpResponse('Unauthorized access', content_type='application/json')
    elif level == 'portfolio':
        if Portfolio.objects.get(pk=level_id).client.user_id != request.user.id:
            return HttpResponse('Unauthorized access', content_type='application/json')
        portfolios = [level_id]
    result = app.get_valuations(portfolios)
    result = json.dumps(result)
    return HttpResponse(result, content_type='application/json')


@user_passes_test()
def getAllValuations(request):
    user = request.user.id
    portfolios = Portfolio.objects.select_related('client').filter(client__user=user)
    portfolio_ids = [x.id for x in portfolios]
    valuations = Valuation.objects.select_related('portfolio').filter(portfolio_id__in=portfolio_ids)
    result = []
    for portfolio in portfolios:
        values = [x.valuation for x in valuations if x.portfolio_id == portfolio.id]
        inflows = [x.netinflows for x in valuations if x.portfolio_id == portfolio.pk]
        dates = [x.dateval.strftime('%Y-%m-%d') for x in valuations if x.portfolio_id == portfolio.pk]
        result.append({
            "portfolioId": portfolio.id, 
            "portfolioName": portfolio.name, 
            "values": values, 
            "inflows": inflows, 
            "dates": dates
        })
    result = json.dumps(result)
    return HttpResponse(result, content_type='application/json')


@user_passes_test()
def getSensitivity(request):
    level = request.GET['level']
    level_id = request.GET['level_id']
    if level == 'principal':
        if Client.objects.get(pk=level_id).user_id != request.user.id:
            return HttpResponse('Unauthorized access', content_type='application/json')
        match = MatchClientsPortfolios.objects.select_related('portfolio').filter(client_id=level_id)
    elif level == 'portfolio':
        match = MatchClientsPortfolios.objects.filter(portfolio_id=level_id)
        if Portfolio.objects.get(pk=level_id).client.user_id != request.user.id:
            return HttpResponse('Unauthorized access', content_type='application/json')
    portfolios = [{"id": x.portfolio_id, "percent": 1} for x in match]
    sensitivities = app.compute_sensitivity(portfolios)
    result = json.dumps(sensitivities)
    return HttpResponse(result, content_type='application/json')


@user_passes_test()
def getExposures(request):
    user = request.user.id
    level = request.GET['level']
    if level == 'account':
        level_id = request.user.id
    else:
        level_id = request.GET['level_id']
    sql = '    SELECT t.id, t.nbShares * c.price / f.value as gbpvalue, c.flCategory, c.issuer, cat.name3, cat.order \
            FROM         app_transaction t \
            LEFT JOIN     app_clientasset c ON t.asset_id=c.id \
            LEFT JOIN     app_category cat ON c.flCategory=cat.level3 \
            LEFT JOIN     app_forex f ON c.currency_id=f.currency \
            LEFT JOIN     app_portfolio p ON p.id=t.portfolio_id \
            LEFT JOIN     app_client cli ON p.client_id=cli.id \
            WHERE c.price is not null AND c.type<>"fx" AND c.type<>"future" AND f.value is not null '
    if level == 'account':
        sql += 'AND cli.user_id=%s'
    elif level == 'principal':
        if Client.objects.get(pk=level_id).user_id != request.user.id:
            return HttpResponse('Unauthorized access', content_type='application/json')
        sql += 'AND cli.id=%s'
    elif level == 'portfolio':
        if Portfolio.objects.get(pk=level_id).client.user_id != request.user.id:
            return HttpResponse('Unauthorized access', content_type='application/json')
        sql += 'AND t.portfolio_id=%s'
    transactions = Transaction.objects.raw(sql, [level_id])
    allocation, exposure, total = app.compute_allocation(transactions)
    result = json.dumps({"allocation": allocation, "exposure": exposure}, ensure_ascii=False).encode('utf8')
    return HttpResponse(result, content_type='application/json')


@user_passes_test()
def getWatchListAdd(request):
    sql = 'SELECT ANY_VALUE(c.id) as id, c.isin, ANY_VALUE(c.name) as name, ANY_VALUE(c.price) as price \
            FROM         app_transaction t \
            LEFT JOIN     app_clientasset c ON t.asset_id=c.id \
            LEFT JOIN     app_portfolio p ON p.id=t.portfolio_id \
            LEFT JOIN     app_client cli ON p.client_id=cli.id \
            WHERE cli.user_id=%s GROUP BY c.isin'
    [conn, cur, curobj] = c.connect_loc()
    curobj.execute(sql, request.user.id)
    transactions = curobj.fetchall()
    conn.close()
    result = json.dumps(transactions)
    return HttpResponse(result, content_type='application/json')
    
    principals = Client.objects.filter(user_id=user).all()
    families = {}
    for principal in principals:
        families[principal.family] = 1
    result = json.dumps([key for key in families.keys()])
    return HttpResponse(result, content_type='application/json')


@user_passes_test()
def getFamilySummary(request): 
    level = request.GET['level']
    level_id  = request.GET['level_id']
    print('getfamilysummary level_id', level_id)
    if level == 'principal':
        sqlTail = 'cli.id=%s'
        expenses = Expense.objects.select_related('client').filter(client_id=level_id).filter(client__user_id=request.user.id)
        baseCurrency = Client.objects.get(pk=level_id).baseCurrency.currency
    else:
        sqlTail = 'cli.family=%s'
        expenses = Expense.objects.select_related('client').filter(client__family=level_id).filter(client__user_id=request.user.id)
        baseCurrency = request.user.user.baseCurrency.currency
    fx = Forex.objects.get(currency=baseCurrency).value
    sql = '''    SELECT t.id, t.nbShares * c.price / f.value * %s as gbpvalue, c.flCategory, c.issuer, cat.name3, cat.order \
            FROM         app_transaction t \
            LEFT JOIN     app_clientasset c ON t.asset_id=c.id \
            LEFT JOIN     app_category cat ON c.flCategory=cat.level3 \
            LEFT JOIN     app_forex f ON c.currency_id=f.currency \
            LEFT JOIN     app_portfolio p ON p.id=t.portfolio_id \
            LEFT JOIN     app_client cli ON p.client_id=cli.id \
            WHERE c.price is not null AND cli.user_id=%s AND ''' + sqlTail
    transactions = Transaction.objects.raw(sql, [fx, request.user.id, level_id])
    allocation, exposure, total_assets = app.compute_allocation(transactions)
    expenses = expenses.filter(tag__in=['Loan', 'Mandatory'])
    liabilities, total_liabilities = app.compute_liabilities(expenses, total_assets, fx)
    result = json.dumps({"assets": allocation, "liabilities": liabilities, "total_assets": total_assets, "total_liabilities": total_liabilities}, ensure_ascii=False).encode('utf8')
    return HttpResponse(result, content_type='application/json')


@user_passes_test()
def getBenchmark(request):
    portfolio = request.GET['portfolio']
    dates, performances = app.compute_benchmark(portfolio)
    result = json.dumps({"dates": dates, "performances": performances})
    return HttpResponse(result, content_type='application/json')


@user_passes_test()
def getCashflows(request):
    user = request.user.id
    client = request.GET['principal']
    if Client.objects.get(pk=client).user_id != user:
        return HttpResponse('Unauthorized access', content_type='application/json')
    cashflows = Expense.objects.filter(client_id=client).all()
    result = serializers.serialize("json", cashflows)
    return HttpResponse(result, content_type='application/json')


@user_passes_test()
def getExpectedReturn(request):
    user = request.user.id
    level = request.GET['level']
    level_id  = request.GET['level_id']
    d_request = 'projection'
    if level == 'portfolio':
        if Portfolio.objects.get(pk=level_id).client.user_id != user:
            return HttpResponse('unauthorized access', content_type='application/json')
        portfolios = [level_id]
    if level == 'principal':
        if Client.objects.get(pk=level_id).user_id != user:
            return HttpResponse('unauthorized access', content_type='application/json')
        match = MatchClientsPortfolios.objects.select_related('client').filter(client_id=level_id)
        portfolios = [x.portfolio_id for x in match]
    [portfolio, baseCurrency, FX] = d.readPortfolio(portfolios)
    [portfolioValue, ref_alloc, ac, pePtf, liquidAssetClasses, peAssetClasses, weightPE, fxExposure] = d.ptf2ac(portfolio, FX)
    result = d.main(baseCurrency, ref_alloc, liquidAssetClasses, pePtf, portfolioValue, weightPE, fxExposure, 0, d_request)
    result = json.dumps(result)
    return HttpResponse(result, content_type='application/json')


@user_passes_test()
def draastic(request):
    portfolioId = request.GET['portfolio']
    d_request = request.GET['request']
    [portfolio, baseCurrency, FX] = d.readPortfolio([portfolioId])
    [portfolioValue, ref_alloc, ac, pePtf, liquidAssetClasses, peAssetClasses, weightPE, fxExposure] = d.ptf2ac(portfolio, FX)
    result = d.main(baseCurrency, ref_alloc, liquidAssetClasses, pePtf, portfolioValue, weightPE, fxExposure, 0, d_request)
    result = json.dumps(result)
    return HttpResponse(result, content_type='application/json')


@user_passes_test()
def getHistory(request):
    user = request.user.id
    principal = request.GET['principal']
    if Client.objects.get(pk=principal).user_id != user:
        return HttpResponse('unauthorized access', content_type='application/json')
    match = MatchClientsPortfolios.objects.select_related('portfolio').filter(client_id=principal)
    portfolios = [x.portfolio_id for x in match]
    transactions = Transaction.objects.filter(portfolio_id__in=portfolios).all()
    result = serializers.serialize('json', transactions)
    return HttpResponse(result, content_type='application/json')


@user_passes_test()
def modelling_v2(request):
    user = request.user.id
    if request.user.is_anonymous:
        return HttpResponse('Unauthorized access', content_type='application/json')
    try:
        level = request.GET['choice']   # 'principal', 'portfolio', 'lumpsum'
        value = request.GET['value']    # principal id, portfolio id, portfolio value when level is 'lumpsum'
        nb_years = int(request.GET['nb_years'])     # horizon
        currency = request.GET['currency']
        target_VaR = float(request.GET['VaR'])     # Conditional Value at Risk (CVaR) value
        optimal_flag = True if request.GET['auto']=='true' else False   # build optimal portfolio according to the required CVar value, 'true' or false
    except:
        return HttpResponse('missing parameters', content_type='application/json')
    this_year = DT.today().year
    cashflows = [float(0)] * nb_years

    if level == 'lumpsum':
        # TODO build optimal portfolio with max annual return / max sharpe
        if currency == 'EUR':
            ref_alloc = [0.28982023452848793, 0.03333525662654416, 0.0018291303301835756, 0.00806504578142115, 0.05715241826810453,
                         0.014414431294985628, 0.01510773129266261, 0.01911984055782969, 0.0, 0.043070152971792806, 0.07070710453913633,
                         0.0, 0.0, 0.019688410625471456, 0.0, 0.0, 0.0, 0.0, 0.014000199856836572, 0.010505201167973358, 0.0, 0.0, 0.0,
                         0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
                         0.0, 0.02146853304124825, 0.37409589598800447, 0.007620413129317387]
        else:
            ref_alloc = [0.05715241826810453, 0.03333525662654416, 0.0018291303301835756, 0.00806504578142115, 0.28982023452848793,
                         0.014414431294985628, 0.01510773129266261, 0.01911984055782969, 0.0, 0.043070152971792806, 0.07070710453913633,
                         0.0, 0.0, 0.019688410625471456, 0.0, 0.0, 0.0, 0.0, 0.014000199856836572, 0.010505201167973358, 0.0, 0.0, 0.0,
                         0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
                         0.0, 0.02146853304124825, 0.37409589598800447, 0.007620413129317387]
        categories = ['eqeur', 'reits', 'eqchf', 'equk', 'eqdev', 'eqasi', 'eqmrg', 'equs', 'bdeur', 'bdwld', 'bdus',
                      'har', 'gold', 'mmeur']
        ref_alloc_another = [0.289820234528488, 0.0191198405578297, 0.00806504578142115, 0.00182913033018358,
                             0.0333352566265442, 0.0151077312926626, 0.0144144312949856, 0.0571524182681045,
                             0.0430701529717928, 0.0707071045391363, 0.0196884106254715, 0.0105052011679734,
                             0.0140001998568366, 0.374095895988005]
        ref_alloc = np.array(ref_alloc)
        pePtf = []
        weightPE = 0
        ptf_value = float(value)
        fxExposure = {}
        liquidAssetClasses = [{'category': cat, 'value': coef * ptf_value}
                              for cat, coef in zip(categories, ref_alloc_another)]
        ptf_currency = currency
    else:
        if level == 'principal':
            if Client.objects.get(pk=value).user_id != user:
                return HttpResponse('unauthorized access', content_type='application/json')
            match = MatchClientsPortfolios.objects.select_related('portfolio').filter(client_id=value)
            expenses = Expense.objects.filter(client_id=value).all()
            for expense in expenses:
                if expense.year >= this_year and expense.year < this_year + nb_years:
                    cashflows[expense.year - this_year] += float(expense.amount)
        if level == 'portfolio':
            match = MatchClientsPortfolios.objects.filter(portfolio_id=value)
        portfolios_id = [x.portfolio_id for x in match]
        lumpsum = 0
        for portfolio_id in portfolios_id:
            valuation = Valuation.objects.filter(portfolio_id=portfolio_id).last()
            lumpsum += valuation.valuation
        [ptf, ptf_currency, ptf_fx_rate] = d.readPortfolio(portfolios_id)
        [ptf_value, ref_alloc, ac, pePtf, liquidAssetClasses, peAssetClasses, weightPE, fxExposure] = d.ptf2ac(ptf, ptf_fx_rate)
    ptf_value_in_selected_cur = float( getFXSpot(ptf_currency, currency) ) * ptf_value
    result = d.var_calc(currency, liquidAssetClasses, ptf_value_in_selected_cur, target_VaR, optimal_flag, nb_years)

    result = json.dumps(result)
    return HttpResponse(result, content_type='application/json')


@user_passes_test()
def modelling(request):
    user = request.user.id
    if request.user.is_anonymous:
        return HttpResponse('Unauthorized access', content_type='application/json')
    try:
        choice = request.GET['choice']
        value = request.GET['value']
        nb_years = int(request.GET['nb_years'])
        currency = request.GET['currency']
        VaR = float(request.GET['VaR'])
        auto = request.GET['auto']
    except:
        return HttpResponse('missing parameters', content_type='application/json')
    this_year = DT.today().year
    d_request = 'match'
    if auto == 'true':
        d_request = 'fit'
    cashflows = [float(0)] * nb_years
    if choice == 'lumpsum':
        if currency == 'EUR':
            ref_alloc = [0.28982023452848793, 0.03333525662654416, 0.0018291303301835756, 0.00806504578142115, 0.05715241826810453, 0.014414431294985628, 0.01510773129266261, 0.01911984055782969, 0.0, 0.043070152971792806, 0.07070710453913633, 0.0, 0.0, 0.019688410625471456, 0.0, 0.0, 0.0, 0.0, 0.014000199856836572, 0.010505201167973358, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.02146853304124825, 0.37409589598800447, 0.007620413129317387]
        else:
            ref_alloc = [ 0.05715241826810453, 0.03333525662654416, 0.0018291303301835756, 0.00806504578142115, 0.28982023452848793, 0.014414431294985628, 0.01510773129266261, 0.01911984055782969, 0.0, 0.043070152971792806, 0.07070710453913633, 0.0, 0.0, 0.019688410625471456, 0.0, 0.0, 0.0, 0.0, 0.014000199856836572, 0.010505201167973358, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.02146853304124825, 0.37409589598800447, 0.007620413129317387]
        ref_alloc = np.array(ref_alloc)
        pePtf = []
        weightPE = 0
        portfolioValue = float(value)
        fxExposure = {}
        liquidAssetClasses =  [
            {'category': 'eqeur', 'value': 0.289820234528488 * portfolioValue},
            {'category': 'reits', 'value': 0.0191198405578297 * portfolioValue},
            {'category': 'eqchf', 'value': 0.00806504578142115 * portfolioValue},
            {'category': 'equk', 'value': 0.00182913033018358 * portfolioValue},
            {'category': 'eqdev', 'value': 0.0333352566265442 * portfolioValue},
            {'category': 'eqasi', 'value': 0.0151077312926626 * portfolioValue},
            {'category': 'eqmrg', 'value': 0.0144144312949856 * portfolioValue},
            {'category': 'equs',  'value': 0.0571524182681045 * portfolioValue},
            {'category': 'bdeur', 'value': 0.0430701529717928 * portfolioValue},
            {'category': 'bdwld', 'value': 0.0707071045391363 * portfolioValue},
            {'category': 'bdus',  'value': 0.0196884106254715 * portfolioValue},
            {'category': 'har',   'value': 0.0105052011679734 * portfolioValue},
            {'category': 'gold',  'value': 0.0140001998568366 * portfolioValue},
            {'category': 'mmeur', 'value': 0.374095895988005 * portfolioValue},
        ]
    else:
        if choice == 'principal':
            if Client.objects.get(pk=value).user_id != user:
                return HttpResponse('unauthorized access', content_type='application/json')
            match = MatchClientsPortfolios.objects.select_related('portfolio').filter(client_id=value)
            expenses = Expense.objects.filter(client_id=value).all()
            for expense in expenses:
                if expense.year >= this_year and expense.year < this_year + nb_years:
                    cashflows[expense.year - this_year] += float(expense.amount)
        if choice == 'portfolio':
            match = MatchClientsPortfolios.objects.filter(portfolio_id=value)
        portfolios = [{"id": x.portfolio_id, "percent": 1} for x in match]
        lumpsum = 0
        for portfolio in portfolios:
            valuation = Valuation.objects.filter(portfolio_id=portfolio['id']).last()
            lumpsum += valuation.valuation
        [portfolio, baseCurrency, FX] = d.readPortfolio([x['id'] for x in portfolios])
        [portfolioValue, ref_alloc, ac, pePtf, liquidAssetClasses, peAssetClasses, weightPE, fxExposure] = d.ptf2ac(portfolio, FX)
    [ptf, rangeVaR] = d.main(currency, ref_alloc, liquidAssetClasses, pePtf, portfolioValue, weightPE, fxExposure, VaR, d_request)
    d_request = 'projection'
    cashflows = [x + portfolioValue for x in cashflows]
    [investment, VaR] = d.simulation(ptf, nb_years, cashflows)
    result = {}
    result['allocation'] = ptf
    result['investment'] = investment
    result['VaR'] = VaR
    result = json.dumps(result)
    return HttpResponse(result, content_type='application/json')


@user_passes_test()
def getReport(request):
    requestId = request.GET['requestId']
    selected_report = Report.objects.get(pk=requestId)
    if selected_report.user_id != request.user.id:
        return HttpResponse('Unauthorized access')
    if selected_report.reportType == 'principal':
        match = MatchClientsPortfolios.objects.select_related('portfolio').filter(client_id=selected_report.reportId)
    elif selected_report.reportType == 'portfolio':
        match = MatchClientsPortfolios.objects.filter(portfolio_id=selected_report.reportId)
    elif selected_report.reportType == 'fund': ## TODO Create dummy portfolio?
        match = []
    elif requestType == 'family': ## TODO Create dummy portfolio?
        selected_report.reportType
    portfolios = [{"id": x.portfolio_id, "percent": 1} for x in match]
    items = selected_report.content
    result = report.create_report(portfolios, items)
    response = app.export_excel(result, request)
    return response


def sendMail(request):
    subject = request.GET['subject']
    message = request.GET['message']
    send_mail(
        subject,
        message,
        'alfred@mail.finlight.com',
        [request.user.email],
        fail_silently=False,
    )
    return HttpResponse('email sent', content_type='application/json')


@user_passes_test()
def upload(request):
    if request.user.is_anonymous:
        return HttpResponse('Unauthorized access', content_type='application/json')
    if request.method == 'POST':
        if  'file' in request.POST:
            file = File(user_id=request.user.user.id, file=request.POST['file'], filetype_id= request.POST['fileId'], mongo=request.POST['mongo'])
            file.save()
            Activity.objects.create(
                action = "Document Uploaded",
                description = request.POST['file'],
                category = request.POST['fileId'],
                user = User.objects.get(id=request.user.id),
                file = file
            )
        else:
            file_object = request.FILES['file']
            file = File(user_id=request.user.user.id, file=file_object, filetype_id=request.POST['fileId'])
            file.save()
        Logs.objects.create(
            action="Document Uploaded",
            date=datetime.date.today(),
            details= file.id, 
            user_id=User.objects.get(id=request.user.id)
        )
    return HttpResponse(file.file.path.split('/')[-1], content_type='application/json')


def getUpload(request):
    files = File.objects.filter(user_id=request.user.user.id).select_related('user').all()
    result = [{'id': x.id, 'name': x.file.name, 'date': x.date.strftime('%Y-%m-%d'), 'user': x.user.user.first_name + ' ' + x.user.user.last_name} for x in files]
    result=json.dumps(result)
    return HttpResponse(result, content_type='application/json')


@user_passes_test()
def download(request):
    file_id = request.GET['id']
    file = File.objects.get(pk=file_id)
    if request.user.is_superuser != 1 and file.user_id != request.user.id:
        return HttpResponse('Unauthorized access', content_type='application/json')
    file_path = file.file.path
    if os.path.exists(file_path):
        with open(file_path, 'rb') as fh:
            response = HttpResponse(fh.read(), content_type="application/pdf")
            response['Content-Disposition'] = 'inline; filename=' + os.path.basename(file_path)
            return response
    raise Http404


@user_passes_test()
def scenario(request):
    choice = request.GET['choice']
    value = request.GET['value']    
    scenario = request.GET['scenario']
    if choice == 'principal':
        if Client.objects.get(pk=value).user_id != request.user.id:
            return HttpResponse('Unauthorized access', content_type='application/json')
        match = MatchClientsPortfolios.objects.select_related('client').filter(client_id=value)
        portfolios = [x.portfolio_id for x in match]
    elif choice == 'portfolio':
        if Portfolio.objects.get(pk=value).client.user_id != request.user.id:
            return HttpResponse('Unauthorized access', content_type='application/json')
        portfolios = [value]
    result = stress.get_assets(portfolios)
    output = [x for x in result if scenario in x.keys()]
    return HttpResponse(json.dumps(output), content_type='application/json')



@user_passes_test()
def getReport_data(request):
    requestId = request.GET['requestId']
    selected_report = Report.objects.get(pk=requestId)
    if selected_report.user_id != request.user.id:
        return HttpResponse('Unauthorized access')
    if selected_report.reportType == 'principal':
        match = MatchClientsPortfolios.objects.select_related('portfolio').filter(client_id=selected_report.reportId)
    elif selected_report.reportType == 'portfolio':
        match = MatchClientsPortfolios.objects.filter(portfolio_id=selected_report.reportId)
    elif selected_report.reportType == 'fund': ## TODO Create dummy portfolio?
        match = []
    elif requestType == 'family': ## TODO Create dummy portfolio?
        selected_report.reportType
    portfolios = [{"id": x.portfolio_id, "percent": 1} for x in match]
    items = selected_report.content
    result = report.create_report(portfolios, items)
    result = json.dumps(result)
    return HttpResponse(result, content_type='application/json')


@user_passes_test()
def getFamilies(request):
    user = request.user.id
    principals = Client.objects.filter(user_id=user).all()
    families = {}
    for principal in principals:
        families[principal.family] = 1
    result = json.dumps([key for key in families.keys()])
    return HttpResponse(result, content_type='application/json')


@user_passes_test()
def all_activities(request):
    user = request.user.id
    activities = Activity.objects.filter(Q(action="New Portfolio") | Q(action="Document Uploaded")| Q(action="replace")| Q(action="update"), user_id=user).order_by('-date').all()
    result = [{'id': x.id, 'action': x.action,'date': x.date.strftime('%d %B %Y'), 'time': x.date.strftime('%H:%M'),
    'description': x.description, 'category': x.category, 'user_id': x.user_id, 'mongo': File.objects.get(id=x.file_id).mongo if x.file_id else x.file_id} for x in activities]
    result = json.dumps(result)
    return HttpResponse(result, content_type='application/json')


@user_passes_test()
def delete_activity(request):
    response = HttpResponse('Something went wrong', status=403)
    if request.method == "POST":
        id = request.POST['id']
        Activity.objects.filter(pk=id).delete()
        response = HttpResponse('Activity deleted', content_type='application/json', status=200)
    elif request.method =="GET":
        response = HttpResponse('Request type forbidden', status=400)
    return response


@user_passes_test()
def new_portfolio(request):
    # Create new portfolio
    # Input: name - portfolio_name  / str
    #        client - principal id / int
    #        cur - currency / str
    #        nbShares - def '1'
    #        type - portfolio type / str
    portfolio_name = request.GET.get('name',  None)
    client = Client.objects.get(pk=request.GET['client']) if 'client' in request.GET else None
    currency = Forex.objects.get(currency=request.GET['cur']) if 'cur' in request.GET else None
    portfolio_type = request.GET.get('type', None)
    if not (portfolio_name and client and currency):
        return JsonResponse({"response": False}, status=200)
    portfolio = Portfolio.objects.create(
        name= portfolio_name,
        client_id= client.id,
        currency_id= currency.currency,
        nbShares= 1,
        type= portfolio_type
    )
    Valuation.objects.create(
                valuation= 0, 
                netinflows= 0, 
                dateval= datetime.date.today(),
                portfolio= portfolio
            )
    MatchClientsPortfolios.objects.create(
                client= client, 
                nbShares= 1, 
                portfolio= portfolio, 
    )
    return JsonResponse({"response": True}, status=200)


def formatNumberWithCcy(value, currency, decimals=2):
    value = float(value)

    if currency == 'EUR':
        currency = '€'
    if currency == 'USD':
        currency = '$'
    if currency == 'GBP':
        currency = '£'
    if currency == 'JPY':
        currency = '¥'

    # print(f'formatNumberWithCcy returning {currency} {value:.2f}')
    return currency + f'{value:.2f}'


def get_peers_benchmark(request):
    # use demo portfolio as a benchmark
    demo_portfolio_id = 11      # could be changed, need a better way to identify the 'demo portfolio'
    probability = 1             # probability value in percent for VaR calculation
    portfolio_id = int(request.GET['portfolio_id']) if 'portfolio_id' in request.GET else None
    if not portfolio_id:
        return HttpResponse(json.dumps([]), content_type='application/json')

    # get portfolio holdings (transactions) with calculated values per holding
    ptf_holdings = calc_holdings(request.user.id, 'portfolio', portfolio_id)
    demo_holdings = calc_holdings(request.user.id, 'portfolio', demo_portfolio_id)

    # get valuations time-series
    ptf_valuations = app.get_valuations([portfolio_id])
    demo_valuations = app.get_valuations([demo_portfolio_id])

    #checking empty valuations
    df_merge = pd.DataFrame()
    if ptf_valuations and demo_valuations:
        # convert valuations times-series to pandas dataframes
        df_ptf = pd.DataFrame(ptf_valuations)
        df_ptf.set_index('dates', inplace=True)
        df_ptf = df_ptf[['values']].rename(columns={'values': 'Portfolio'})
        # dropping any rows with zeros (for valuation and inflows)
        df_ptf = df_ptf[(df_ptf != 0).any(axis=1)]
        df_demo = pd.DataFrame(demo_valuations)
        df_demo.set_index('dates', inplace=True)
        df_demo = df_demo[['values']].rename(columns={'values': 'Peers Benchmark'})
        # dropping any rows with zeros (for valuation and inflows)
        df_demo = df_demo[(df_demo != 0).any(axis=1)]
        # merge both valuations dataframes and drop NaN in case start dates are not matching
        df_merge = pd.concat([df_ptf, df_demo], axis=1).dropna()

        if not df_merge.empty:
            # use first valuation value for percentage calculation
            for ptf_name in df_merge.columns:
                df_merge[ptf_name] = df_merge[ptf_name] / df_merge[ptf_name][0] * 100

    categories_mapping = {
                          'eq': 'Equity',
                          'mm': 'Cash',
                          'reits': 'Property',
                          'h': 'Hedge funds',
                          'bd': 'Bonds',
                          'pe': 'Private equity'
                          }
    categories = list(categories_mapping.values()) + ['Other']

    # remap holdings categories for chosen portfolio to the list of only 6 categories
    ptf_values = {cat: 0 for cat in categories}
    for item in ptf_holdings:
        category = "Other"
        for category_starts_with in categories_mapping:
            if item['category_short'].startswith(category_starts_with):
                category = categories_mapping[category_starts_with]
        if category not in ptf_values:
            ptf_values[category] = 0
        ptf_values[category] += item['valueInPortfolioCurrency']
    # Working with empty portfolio. Set total value to 1, while weights are set to 0 (zero)
    ptf_total_value = sum(ptf_values.values()) if ptf_holdings else 1


    # remap holdings categories for demo portfolio to the list of only 6 categories
    demo_values = {cat: 0 for cat in categories}
    for item in demo_holdings:
        category = "Other"
        for category_starts_with in categories_mapping:
            if item['category_short'].startswith(category_starts_with):
                category = categories_mapping[category_starts_with]
        if category not in demo_values:
            demo_values[category] = 0
        demo_values[category] += item['valueInPortfolioCurrency']
    # Working with empty portfolio. Set total value to 1, while weights are set to 0 (zero)
    demo_total_value = sum(demo_values.values()) if demo_holdings else 1

    # calc value weights per category
    ptf_alloc = {cat: float(value / ptf_total_value) for cat, value in ptf_values.items()}
    demo_alloc = {cat: float(value / demo_total_value) for cat, value in demo_values.items()}
    ptf_alloc = pd.Series(ptf_alloc)
    demo_alloc = pd.Series(demo_alloc)
    alloc_diff2 = ptf_alloc - demo_alloc


    # checking empty dataframe
    if not df_merge.empty:
        # load historic 'category' prices starting from the min date
        min_date = df_merge.index[0]
        prices = get_histo_prices('*', start_date=min_date)

        # aggregate prices into only 6 categories
        df_aggr_prices = pd.DataFrame(columns=categories, index=prices.index).fillna(0)
        for price_cat in prices.columns:
            category = 'Other'
            for category_starts_with in categories_mapping:
                if price_cat.startswith(category_starts_with):
                    category = categories_mapping[category_starts_with]
            df_aggr_prices[category] += prices[price_cat]

        # if PE index is not recorded in the histo_index table, all return values are equal to zero
        # calc percentage returns
        returns = df_aggr_prices.pct_change()[1:]

        # calc annual VaR using daily VaR. Fill NA with zeros in case of missing data, eg for 'Private Equity'
        var_per_category = (returns.quantile(probability/100) * np.sqrt(252)).fillna(0)

        # calc weighted VaR
        ptf_VaR = abs(var_per_category * ptf_alloc)
        demo_VaR = abs(var_per_category * demo_alloc)

        # calc 'category' performance as a diff between last and first (100%) values and multiply by weights
        ptf_perf = (df_merge['Portfolio'][-1] - 100) * ptf_alloc
        demo_perf = (df_merge['Peers Benchmark'][-1] - 100) * demo_alloc
    else:
        ptf_VaR = pd.Series( dict(zip(categories, [0]*len(categories))) )
        demo_VaR = pd.Series( dict(zip(categories, [0]*len(categories))) )

    res = {
        'values': df_merge.to_dict(),
        'allocation':
        {
            'Portfolio': (ptf_alloc * 100).to_dict(),
            'Peers Benchmark': (demo_alloc * 100).to_dict()
        },
        'allocation_difference': alloc_diff2.to_dict(),
        'var':
        {
            'Portfolio': sum(ptf_VaR * 100),
            'Peers Benchmark': sum(demo_VaR * 100)
        },
        'var_per_category':
        {
            'Portfolio': (ptf_VaR * 100).to_dict(),
            'Peers Benchmark': (demo_VaR * 100).to_dict()
        },
        'performance':
        {
            'Portfolio': alloc_diff2.to_dict(),                         # Allocation values
            'Peers Benchmark': dict(zip(categories, [-.008, 0, .005, .003, -.001, 0, 0]))   # Selection values
        }
    }

    return HttpResponse(json.dumps(res), content_type='application/json')


def settings_email(request):
    return HttpResponse('create', content_type='application/json')


def newUser(request):
    response = HttpResponse('Something went wrong', content_type='application/json', status=403)
    if request.method == 'POST':
        username    = request.POST.get('username')
        first_name  = request.POST.get('firstName')
        last_name   = request.POST.get('lastName')
        email       = request.POST.get('email')
        password = ''.join(random.choice(string.printable) for i in range(12))
        if auth_User.objects.filter(email=email).exists():
            response = HttpResponse('A user with that email already exists', content_type='application/json', status=400)
        else:
            new_user = auth_User.objects.create_user(
                username= username,
                first_name= first_name,
                last_name= last_name,
                email= email,
                password= password,
                is_active= False
                )
            dashboard_value = request.POST.get('dashboard')
            activity_value = request.POST.get('activity')
            upload_value = request.POST.get('upload')
            private_equity_value = request.POST.get('private_equity')
            portfolios_value = request.POST.get('portfolios')
            principals_value = request.POST.get('principals')
            reports_value = request.POST.get('reports')
            modelling_value = request.POST.get('modelling')
            peers_benchmark_value = request.POST.get('peers_benchmark')
            superuser_id = request.user.id
            access_type = request.POST.get('access_type')
            path = request.build_absolute_uri()
            permission = {
                'new_user': new_user,
                'superuser': superuser_id, 
                'dashboard': dashboard_value, 
                'activity': activity_value, 
                'upload': upload_value,
                'private_equity': private_equity_value,
                'portfolios': portfolios_value,
                'principals': principals_value,
                'reports': reports_value, 
                'modelling': modelling_value, 
                'peers_benchmark': peers_benchmark_value,
                'access_type': access_type
            }
            add_user_permissions(permission, path)
            response = HttpResponse('user created, invitation sent', content_type='application/json', status=200)
    else: 
        response = HttpResponse('Request type forbidden', content_type='application/json', status=400)
    return response



def create_user(request):
    response = HttpResponse('Something went wrong', content_type='application/json', status=403)
    if request.method == 'POST':
        username    = request.POST.get('username')
        first_name  = request.POST.get('firstName')
        last_name   = request.POST.get('lastName')
        email       = request.POST.get('email')
        password = request.POST.get('password')
        if auth_User.objects.filter(email=email).exists():
            response = HttpResponse('A user with that email already exists', content_type='application/json', status=400)
        else:
            new_user = auth_User.objects.create_user(
                username= username,
                first_name= first_name,
                last_name= last_name,
                email= email,
                password= password,
                is_active= False
                )
            response = HttpResponse('Good', content_type='application/json', status=200)
    return response


def checkUser(request):
    response = HttpResponse('Something went wrong', content_type='application/json', status=403)
    if request.method == 'POST':
        print('ok for method')
        username    = request.POST.get('email')
        print(username)
        password = request.POST.get('password')
        test = auth_User.objects.get(email=username).id
        print(test)
        response = HttpResponse(test,content_type='application/json', status=200)
    return response



def add_user_permissions(permission, path):
    superuser = auth_User.objects.get(id=permission['superuser'])
    dashboard = Permission.objects.get(value=permission['dashboard'])
    activity = Permission.objects.get(value=permission['activity'])
    upload = Permission.objects.get(value=permission['upload'])
    private_equity = Permission.objects.get(value=permission['private_equity'])
    portfolios = Permission.objects.get(value=permission['portfolios'])
    principals = Permission.objects.get(value=permission['principals'])
    reports = Permission.objects.get(value=permission['reports'])
    modelling = Permission.objects.get(value=permission['modelling'])
    peers_benchmark = Permission.objects.get(value=permission['peers_benchmark'])
    user_access = UserPermission.objects.get(user=superuser)
    if user_access.access_type == 'superuser':
        UserPermission.objects.create(
            dashboard = dashboard,
            activity = activity,
            upload = upload,
            private_equity = private_equity,
            portfolios = portfolios,
            principals = principals,
            reports = reports,
            modelling = modelling,
            peers_benchmark = peers_benchmark,
            superuser = superuser,
            user = permission['new_user'],
            access_type = permission['access_type']
        )
        generate_invite(permission['new_user'], path)


def generate_invite(new_user, path):
    uid = new_user.id
    token = default_token_generator.make_token(new_user)
    local_1 = '127.0.0.1:8000/'
    local_2 = 'localhost'
    demo = 'demo.finlight.com/'
    app = 'app.finlight.com/'
    if (local_1 in path) or (local_2 in path):
        url = f'http://127.0.0.1:8000/app/verifyaccount?token={token}&uid={uid}'
    elif demo in path:
        url = f'https://demo.finlight.com/app/verifyaccount?token={token}&uid={uid}'
    elif app in path:
        url = f'https://app.finlight.com/app/verifyaccount?token={token}&uid={uid}'
    else:
        url = 'http://error?=string-not-found-in-path'
    send_invite(new_user, url)


def send_invite(new_user, url):
    invite_first_name = new_user.first_name
    email = new_user.email
    invite_link = url
    print(url)
    url = 'https://e.finlight.com/invite/'
    data = {'invite_first_name': invite_first_name, 'email': email, 'invite_link': invite_link}
    r = requests.post(
        url=url,
        data=dict(
            invite_first_name=invite_first_name,
            email=email,
            invite_link=invite_link
            ),
    )
    print(r.text)


def finalise_invite(request):
    response = HttpResponse('An error occured, please contact your administrator', content_type='application/json', status=403)
    try: 
        uid = request.GET.get('uid')
        user = auth_User.objects.get(id=uid)
        user_id = user.id
        token = request.GET.get('token')
        if default_token_generator.check_token(user, token):
            user.is_active = True
            user.password = ''
            user.save()
            response = HttpResponse('Account Verified', content_type='application/json', status=200)
        else:
            response = HttpResponse('Invalid token', content_type='application/json', status=400)
    except: 
        response = HttpResponse('Cannot verify account, please contact your administrator', content_type='application/json', status=400)
    return response


def activity_invitation(request):
    if request.method == "POST":
        firstname = request.POST.get('firstname')
        lastname = request.POST.get('lastname')
        file = None
        Activity.objects.create(
            action = 'New Invitation Sent',
            description = f'{firstname} {lastname} has been invited',
            category = 'invitation',
            user = User.objects.get(id=request.user.id),
            file = file
        )
        response = HttpResponse('Activity added', content_type='application/json', status=200)
    else:
        response = HttpResponse('Error: activity could not be added', content_type='application/json', status=400)
    return response

