#!/usr/bin/env python3

# Test what the broker does if receiving a PUBCOMP in response to a QoS 1 PUBLISH.

from mosq_test_helper import *

def helper(port, proto_ver):
    connect_packet = mosq_test.gen_connect("helper", keepalive=60, proto_ver=proto_ver)
    connack_packet = mosq_test.gen_connack(rc=0, proto_ver=proto_ver)

    mid = 1
    publish1s_packet = mosq_test.gen_publish("subpub/qos1", qos=1, mid=mid, payload="message", proto_ver=proto_ver)
    puback1s_packet = mosq_test.gen_puback(mid, proto_ver=proto_ver)

    sock = mosq_test.do_client_connect(connect_packet, connack_packet, timeout=20, port=port)
    mosq_test.do_send_receive(sock, publish1s_packet, puback1s_packet, "puback 1s")
    sock.close()


def do_test(proto_ver):
    rc = 1
    keepalive = 60

    connect_packet = mosq_test.gen_connect("subpub-qos1-test", keepalive=keepalive, proto_ver=proto_ver)
    connack_packet = mosq_test.gen_connack(rc=0, proto_ver=proto_ver)

    mid = 1
    subscribe_packet = mosq_test.gen_subscribe(mid, "subpub/qos1", 1, proto_ver=proto_ver)
    suback_packet = mosq_test.gen_suback(mid, 1, proto_ver=proto_ver)

    mid = 1
    publish_packet2 = mosq_test.gen_publish("subpub/qos1", qos=1, mid=mid, payload="message", proto_ver=proto_ver)

    mid = 1
    publish1r_packet = mosq_test.gen_publish("subpub/qos1", qos=1, mid=mid, payload="message", proto_ver=proto_ver)
    pubcomp1r_packet = mosq_test.gen_pubcomp(mid, proto_ver=proto_ver)

    pingreq_packet = mosq_test.gen_pingreq()
    pingresp_packet = mosq_test.gen_pingresp()

    port = mosq_test.get_port()
    broker = mosq_test.start_broker(filename=os.path.basename(__file__), port=port)

    try:
        sock = mosq_test.do_client_connect(connect_packet, connack_packet, timeout=20, port=port)
        mosq_test.do_send_receive(sock, subscribe_packet, suback_packet, "suback")

        helper(port, proto_ver)

        mosq_test.expect_packet(sock, "publish 1r", publish1r_packet)
        sock.send(pubcomp1r_packet)
        sock.send(pingreq_packet)
        p = sock.recv(len(pingresp_packet))
        if len(p) == 0:
            rc = 0

        sock.close()
    except socket.error as e:
        if e.errno == errno.ECONNRESET:
            # Connection has been closed by peer, this is the expected behaviour
            rc = 0
    except mosq_test.TestError:
        pass
    finally:
        broker.terminate()
        broker.wait()
        (stdo, stde) = broker.communicate()
        if rc:
            print(stde.decode('utf-8'))
            print("proto_ver=%d" % (proto_ver))
            exit(rc)


do_test(proto_ver=4)
do_test(proto_ver=5)
exit(0)
