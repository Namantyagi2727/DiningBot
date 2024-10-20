import math
import dateutil.parser
import datetime
import time
import os
import logging
import boto3


logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
SQS = boto3.client("sqs")

""" --- Helpers to build responses which match the structure of the necessary dialog actions --- """

def getQueueURL():
    """Retrieve the URL for the configured queue name"""
    q = SQS.get_queue_url(QueueName='DiningConciergeQueue').get('QueueUrl')
    return q

def record(event):
    """The lambda handler"""
    logger.debug("Recording with event %s", event)
    try:
        slots = get_slots(event)
        logger.debug("Recording %s", slots)
        
        u = getQueueURL()
        logging.debug("Got queue URL %s", u)
        
        resp = SQS.send_message(
            QueueUrl=u,  
            MessageBody="Dining Concierge message from LF1",
            MessageAttributes={
                "Location": {
                    "StringValue": str(slots["Location"]),
                    "DataType": "String"
                },
                "Cuisine": {
                    "StringValue": str(slots["Cuisine"]),
                    "DataType": "String"
                },
                "DiningDate": {
                    "StringValue": slots["DiningDate"],
                    "DataType": "String"
                },
                "DiningTime": {
                    "StringValue": str(slots["DiningTime"]),
                    "DataType": "String"
                },
                "NumberOfPeople": {
                    "StringValue": str(slots["NumberOfPeople"]),
                    "DataType": "String"
                },
                "Email": {
                    "StringValue": str(slots["Email"]),
                    "DataType": "String"
                }
            }
        )
        logger.debug("Send result: %s", resp)
    except Exception as e:
        raise Exception("Could not record link! %s" % e)

def get_slots(intent_request):
    return intent_request['currentIntent']['slots']

def elicit_slot(session_attributes, intent_name, slots, slot_to_elicit, message):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'ElicitSlot',
            'intentName': intent_name,
            'slots': slots,
            'slotToElicit': slot_to_elicit,
            'message': message
        }
    }

def close(session_attributes, fulfillment_state, message):
    response = {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Close',
            'fulfillmentState': fulfillment_state,
            'message': message
        }
    }

    return response

def delegate(session_attributes, slots):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Delegate',
            'slots': slots
        }
    }

""" --- Helper Functions --- """
def isvalid_date(date):
    try:
        dateutil.parser.parse(date)
        return True
    except ValueError:
        return False

def parse_int(n):
    try:
        return int(n)
    except ValueError:
        return float('nan')

def build_validation_result(is_valid, violated_slot, message_content):
    if message_content is None:
        return {
            "isValid": is_valid,
            "violatedSlot": violated_slot,
        }

    return {
        'isValid': is_valid,
        'violatedSlot': violated_slot,
        'message': {'contentType': 'PlainText', 'content': message_content}
    }

def validate_dining_suggestion(location, cuisine, dining_time, dining_date, number_of_people, email):
    locations = ['new york']
    if location is not None and location.lower() not in locations:
        return build_validation_result(
            False,
            'Location',
            'We do not have suggestions for {}, would you like suggestions for a different location? '
            'Try searching for New York?'.format(location)
        )

    cuisines = ['chinese', 'lebanese', 'italian', 'japanese', 'mexican']
    if cuisine is not None and cuisine.lower() not in cuisines:
        return build_validation_result(
            False,
            'Cuisine',
            'We do not have suggestions for {}, would you like suggestions for a different cuisine? '
            'Popular cuisines are Chinese, Lebanese, Japanese, Italian, or Mexican.'.format(cuisine)
        )

    if dining_date is not None:
        if not isvalid_date(dining_date):
            return build_validation_result(False, 'DiningDate', 'I did not understand that, what date would you like to dine?')
        elif datetime.datetime.strptime(dining_date, '%Y-%m-%d').date() < datetime.date.today():
            return build_validation_result(False, 'DiningDate', 'Sorry, you cannot choose a past date. What date would you like?')

    if dining_time is not None:
        if len(dining_time) != 5:
            return build_validation_result(False, 'DiningTime', 'Please specify a valid time.')
        
        hour, minute = dining_time.split(':')
        hour = parse_int(hour)
        minute = parse_int(minute)
        if math.isnan(hour) or math.isnan(minute):
            return build_validation_result(False, 'DiningTime', 'I did not understand the time. Please provide it in HH:MM format.')
        
        if hour < 10 or hour > 24:
            return build_validation_result(
                False,
                'DiningTime',
                'Our business hours are from 10 AM to 11 PM. Can you specify a time within this range?'
            )

    if number_of_people is not None and not number_of_people.isnumeric():
        return build_validation_result(
            False,
            'NumberOfPeople',
            'That does not look like a valid number {}, could you please repeat?'.format(number_of_people)
        )

    if email is not None and "@" not in email:
        return build_validation_result(
            False,
            'Email',
            'The email address {} seems incorrect. Could you please repeat?'.format(email)
        )

    return build_validation_result(True, None, None)

""" --- Functions that control the bot's behavior --- """

def diningSuggestions(intent_request, context):
    
    location = get_slots(intent_request)["Location"]
    cuisine = get_slots(intent_request)["Cuisine"]
    dining_date = get_slots(intent_request)["DiningDate"]
    dining_time = get_slots(intent_request)["DiningTime"]
    number_of_people = get_slots(intent_request)["NumberOfPeople"]
    email = get_slots(intent_request)["Email"]
    source = intent_request['invocationSource']

    if source == 'DialogCodeHook':
        # Perform basic validation on the supplied input slots.
        # Use the elicitSlot dialog action to re-prompt for the first violation detected.
        slots = get_slots(intent_request)

        validation_result = validate_dining_suggestion(location, cuisine, dining_time, dining_date, number_of_people, email)
        if not validation_result['isValid']:
            slots[validation_result['violatedSlot']] = None
            return elicit_slot(intent_request['sessionAttributes'],
                               intent_request['currentIntent']['name'],
                               slots,
                               validation_result['violatedSlot'],
                               validation_result['message'])

        output_session_attributes = intent_request['sessionAttributes'] if intent_request['sessionAttributes'] is not None else {}
        return delegate(output_session_attributes, get_slots(intent_request))

    # Record information in SQS and close the conversation
    record(intent_request)
    return close(intent_request['sessionAttributes'],
                 'Fulfilled',
                 {'contentType': 'PlainText',
                  'content': 'Thank you for the information. We are generating our recommendations and will send them to your email when ready.'})


""" --- Intents --- """

def welcome(intent_request):
    return close(intent_request['sessionAttributes'],
                 'Fulfilled',
                 {'contentType': 'PlainText',
                  'content': 'Hi there, how can I help you?'})

def thankYou(intent_request):
    return close(intent_request['sessionAttributes'],
                 'Fulfilled',
                 {'contentType': 'PlainText',
                  'content': 'You are welcome!'})

def dispatch(intent_request, context):
    """
    Called when the user specifies an intent for this bot.
    """

    logger.debug('dispatch userId={}, intentName={}'.format(intent_request['userId'], intent_request['currentIntent']['name']))

    intent_name = intent_request['currentIntent']['name']

    # Dispatch to your bot's intent handlers
    if intent_name == 'DiningSuggestionsIntent':
        return diningSuggestions(intent_request, context)
    elif intent_name == 'ThankYouIntent':
        return thankYou(intent_request)
    elif intent_name == 'GreetingIntent':
        return welcome(intent_request)

    raise Exception('Intent with name ' + intent_name + ' not supported')


""" --- Main handler --- """

def lambda_handler(event, context):
    """
    Route the incoming request based on intent.
    The JSON body of the request is provided in the event slot.
    """
    # By default, treat the user request as coming from the America/New_York time zone.
    os.environ['TZ'] = 'America/New_York'
    time.tzset()
    logger.debug('event.bot.name={}'.format(event['bot']['name']))

    return dispatch(event, context)
