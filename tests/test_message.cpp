// Message serialization / parsing, framing over a real socket, SHA-256, base64, helpers.
#include <sys/socket.h>
#include <unistd.h>

#include <thread>

#include "../common/json.h"
#include "../common/message.h"
#include "../common/sha256.h"
#include "../common/util.h"
#include "test_util.h"

using namespace nexus;

int main() {
    TEST_CASE("message round trip with special characters");
    {
        Message m("TASK_RESULT");
        m.set("result", "a|b=c%d\nline2\r").setInt("task_id", 42).setDouble("exec_time", 1.23456);
        std::string wire = m.serialize();
        CHECK(wire.back() == '\n');
        CHECK(wire.find('\n') == wire.size() - 1);          // exactly one line
        Message back;
        CHECK(Message::parse(wire, back));
        CHECK_EQ(back.type(), std::string("TASK_RESULT"));
        CHECK_EQ(back.get("result"), std::string("a|b=c%d\nline2\r"));
        CHECK_EQ(back.getInt("task_id"), 42LL);
        CHECK(back.getDouble("exec_time") > 1.23 && back.getDouble("exec_time") < 1.24);
        CHECK(back.has("task_id"));
        CHECK(!back.has("nope"));
    }

    TEST_CASE("malformed messages are rejected, never crash");
    {
        Message m;
        CHECK(!Message::parse("", m));
        CHECK(!Message::parse("\n", m));
        CHECK(!Message::parse("|a=b", m));                  // empty type
        CHECK(Message::parse("PING", m));                   // type only is fine
        CHECK(Message::parse("PING|novalue|k=%zz|x=%4", m)); // bad escapes survive as literal text
        CHECK_EQ(m.get("k"), std::string("%zz"));
        CHECK_EQ(m.getInt("x", -7), -7LL);                  // not a number -> default
        CHECK_EQ(m.getInt("missing", 5), 5LL);
        Message n;
        CHECK(Message::parse("T|n=abc", n));
        CHECK_EQ(n.getInt("n", 9), 9LL);
        CHECK_EQ(n.getDouble("n", 1.5), 1.5);
    }

    TEST_CASE("framing over a real socket: split writes, many messages, large payload");
    {
        int sv[2];
        CHECK_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, sv), 0);
        Connection a(sv[0]), b(sv[1]);

        // two messages in one write + one message split across writes
        std::string two = Message("A").set("k", "1").serialize() + Message("B").set("k", "2").serialize();
        std::string third = Message("C").set("k", "3").serialize();
        CHECK_EQ(::write(sv[0], two.data(), two.size()), static_cast<ssize_t>(two.size()));
        CHECK_EQ(::write(sv[0], third.data(), 4), static_cast<ssize_t>(4));
        Message m;
        CHECK_EQ(b.receive(m, 500), 1); CHECK_EQ(m.type(), std::string("A"));
        CHECK_EQ(b.receive(m, 500), 1); CHECK_EQ(m.type(), std::string("B"));
        CHECK_EQ(b.receive(m, 100), 0);                     // incomplete line: timeout, not an error
        CHECK_EQ(::write(sv[0], third.data() + 4, third.size() - 4), static_cast<ssize_t>(third.size() - 4));
        CHECK_EQ(b.receive(m, 500), 1); CHECK_EQ(m.type(), std::string("C"));

        // 5 MB payload (a base64 storage block), sent from another thread
        std::string big(5 * 1024 * 1024, 'x');
        for (size_t i = 0; i < big.size(); i += 997) big[i] = static_cast<char>('a' + i % 26);
        std::thread sender([&] { Message x("BLOCK_STORE"); x.set("data", big); a.send(x); });
        int rc = 0;
        for (int i = 0; i < 50 && rc == 0; ++i) rc = b.receive(m, 200);
        sender.join();
        CHECK_EQ(rc, 1);
        CHECK(m.get("data") == big);

        // peer closes -> receive reports -1
        ::close(sv[0]);
        a.shutdown();
        CHECK_EQ(b.receive(m, 500), -1);
    }

    TEST_CASE("SHA-256 known answers");
    {
        CHECK_EQ(sha256Hex(std::string("")), std::string("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"));
        CHECK_EQ(sha256Hex(std::string("abc")), std::string("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"));
        CHECK_EQ(sha256Hex(std::string("abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq")),
                 std::string("248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1"));
        CHECK_EQ(sha256Hex(std::string(1000000, 'a')), std::string("cdc76e5c9914fb9281a1c7e284d73e67f1809a48a497200e046d39ccc7112cd0"));
    }

    TEST_CASE("base64");
    {
        std::string out;
        CHECK_EQ(base64Encode("Man"), std::string("TWFu"));
        CHECK_EQ(base64Encode("Ma"), std::string("TWE="));
        CHECK_EQ(base64Encode("M"), std::string("TQ=="));
        std::string bin;
        for (int i = 0; i < 256; ++i) bin += static_cast<char>(i);
        CHECK(base64Decode(base64Encode(bin), out) && out == bin);
        CHECK(!base64Decode("abc", out));                   // bad length
        CHECK(!base64Decode("ab!d", out));                  // bad character
        CHECK(!base64Decode("=abc", out));                  // padding in the wrong place
    }

    TEST_CASE("params, ids, file names");
    {
        Params p{{"a", "x y&z=1"}, {"b", "%"}};
        Params q = decodeParams(encodeParams(p));
        CHECK(p == q);
        CHECK(isSafeId("ds1_b0"));
        CHECK(!isSafeId("../etc/passwd"));
        CHECK(!isSafeId("a/b"));
        CHECK(!isSafeId(""));
        CHECK(!isSafeId("a.b"));
        CHECK(!isSafeId(std::string(65, 'a')));
        CHECK_EQ(sanitizeFileName("../../etc/passwd"), std::string("passwd"));
        CHECK_EQ(sanitizeFileName("C:\\x\\..\\y.txt"), std::string("y.txt"));
        CHECK_EQ(sanitizeFileName("..."), std::string("file"));
        CHECK_EQ(sanitizeFileName("a b;rm -rf.txt"), std::string("abrm-rf.txt"));
        CHECK_EQ(json::str("a\"b\\c\n"), std::string("\"a\\\"b\\\\c\\n\""));
    }
    return testResult("test_message");
}
