"""Authenticated Graph direct-producer identity for classic delivery settlement."""


def graph_direct_native_row(row):
    """Recognize the Graph adapter's authenticated direct native envelope."""
    payload = row.get('payload') if isinstance(row, dict) else None
    descriptor = payload.get('native_text_v1') if isinstance(payload, dict) else None
    source = descriptor.get('source') if isinstance(descriptor, dict) else None
    automation = descriptor.get('automation') if isinstance(descriptor, dict) else None
    event = descriptor.get('event') if isinstance(descriptor, dict) else None
    principal_id = row.get('principal_id') if isinstance(row, dict) else None
    route = descriptor.get('route') if isinstance(descriptor, dict) else None
    return (
        isinstance(descriptor, dict)
        and descriptor.get('producer_process_mode') == 'direct'
        and isinstance(source, dict)
        and source.get('platform') == 'msgraph_webhook'
        and isinstance(source.get('chat_id'), str)
        and source['chat_id'].startswith('msgraph:')
        and source.get('user_id') == 'msgraph'
        and isinstance(route, str)
        and route
        and isinstance(principal_id, str)
        and principal_id == f'msgraph:{route}'
        and isinstance(automation, dict)
        and automation.get('owner') == row.get('target_session_id')
        and automation.get('identity') == row.get('request_id')
        and isinstance(event, dict)
        and event.get('message_id') == row.get('request_id')
    )
