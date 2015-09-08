require "logstash/devutils/rspec/spec_helper"
require "cabin/rspec/cabin_helper"
require "logstash/plugin"
require "logstash/json"

describe "outputs/riemann" do

  context "registration" do

    it "should register" do
      output = LogStash::Plugin.lookup("output", "riemann").new
      expect {output.register}.not_to raise_error
    end

    context "protocol" do
      it "should fail if not set to [tcp] or [udp]" do
        expect {
          output = LogStash::Plugin.lookup("output", "riemann").new("protocol" => "fake")
          output.register
          }.to raise_error
      end

      it "should not error out if set to [tcp]" do
        expect {
          output = LogStash::Plugin.lookup("output", "riemann").new("protocol" => "tcp")
          output.register
          }.not_to raise_error
      end

      it "should not error out if set to [udp]" do
        expect {
          output = LogStash::Plugin.lookup("output", "riemann").new("protocol" => "udp")
          output.register
          }.not_to raise_error
      end

    end
  end

  context "receive" do

    output = LogStash::Plugin.lookup("output", "riemann").new
    data = {"message"=>"hello", "@version"=>"1", "@timestamp"=>"2015-06-03T23:34:54.076Z", "host"=>"vagrant-ubuntu-trusty-64"}
    event = LogStash::Event.new data

    it "should accept the event" do
      expect { output.receive event }.not_to raise_error
    end
  end

  context "map_fields" do

    context "with basic data" do

      it "will return keys that do not start with @ sign." do
        data = {"message"=>"hello", "@version"=>"1", "@timestamp"=>"2015-06-03T23:34:54.076Z", "host"=>"vagrant-ubuntu-trusty-64"}
        expected_data = {:message=>"hello", :host=>"vagrant-ubuntu-trusty-64"}
        output = LogStash::Plugin.lookup("output", "riemann").new
        expect(output.map_fields(nil, data)).to eq expected_data
      end

      it "will return a hash of nested values" do
        data = {"message"=>"hello", "node_info" => {"name" => "node1", "status" => "up"}, "@version"=>"1", "@timestamp"=>"2015-06-03T23:34:54.076Z", "host"=>"vagrant-ubuntu-trusty-64"}
        expected_data = {:message =>"hello", :host =>"vagrant-ubuntu-trusty-64", :"node_info.name" => "node1", :"node_info.status" => "up"}
        output = LogStash::Plugin.lookup("output", "riemann").new
        expect(output.map_fields(nil, data)).to eq expected_data
      end

    end
  end

  context "build_riemann_formatted_event" do

    context "with map_fields" do

      it "will return symboled hash with at least :host, :time, and :description" do
        data = {"message"=>"hello", "node_info" => {"name" => "node1", "status" => "up"}, "@version"=>"1", "@timestamp"=>"2015-06-03T23:34:54.076Z", "host"=>"vagrant-ubuntu-trusty-64"}
        expected_data = {:time=>1433374494, :message =>"hello", :description =>"hello", :host =>"vagrant-ubuntu-trusty-64", :"node_info.name" => "node1", :"node_info.status" => "up"}
        event = LogStash::Event.new data
        output = LogStash::Plugin.lookup("output", "riemann").new("map_fields" => "true")
        expect(output.build_riemann_formatted_event(event)).to eq expected_data
      end

    end

    context "without map_fields" do

      it "will return symboled hash with at least :host, :time, and :description" do
        data = {"message"=>"hello", "node_info" => {"name" => "node1", "status" => "up"}, "@version"=>"1", "@timestamp"=>"2015-06-03T23:34:54.076Z", "host"=>"vagrant-ubuntu-trusty-64"}
        expected_data = {:time=>1433374494, :description =>"hello", :host =>"vagrant-ubuntu-trusty-64"}
        event = LogStash::Event.new data
        output = LogStash::Plugin.lookup("output", "riemann").new
        expect(output.build_riemann_formatted_event(event)).to eq expected_data
      end

    end

    context "with tags" do
      it "will return a symboled tags with multiple tags" do
        data = {"tags"=> ["good_enough", "smart_enough", "doggone_it", "people_like_me"], "message"=>"hello", "node_info" => {"name" => "node1", "status" => "up"}, "@version"=>"1", "@timestamp"=>"2015-06-03T23:34:54.076Z", "host"=>"vagrant-ubuntu-trusty-64"}
        expected_data = {:tags => ["good_enough", "smart_enough", "doggone_it", "people_like_me"], :time=>1433374494, :description =>"hello", :host =>"vagrant-ubuntu-trusty-64"}
        event = LogStash::Event.new data
        output = LogStash::Plugin.lookup("output", "riemann").new
        expect(output.build_riemann_formatted_event(event)).to eq expected_data
      end
    end

    context "when using a Json string, tags might be an ArrayList" do
      let(:tcp) { double('tcpsocket') }
      let(:json_string) { '{"tags":["good_enough", "smart_enough", "doggone_it", "people_like_me", null], "message":"hello", "node_info":{"name":"node1", "status":"up"}, "@version":"1", "@timestamp":"2015-06-03T23:34:54.076Z", "host":"vagrant-ubuntu-trusty-64"}' }
      let(:event) { LogStash::Event.new(LogStash::Json.load(json_string)) }
      let(:output) { LogStash::Plugin.lookup("output", "riemann").new }

      before do
        #stub out riemann server
        allow(TCPSocket).to receive(:new).and_return(tcp)
        allow(UDPSocket).to receive(:new).and_return(tcp)
        allow(tcp).to receive(:<<)
        # small risk here, this is the riemann response in binary
        # it is the TCP OK response to a send, it is unlikely to change.
        allow(tcp).to receive(:read).with(4).and_return("\x00\x00\x00\x02")
        allow(tcp).to receive(:read).with(2).and_return("\x10\x01")
        output.register
        output.receive(event)
      end

      it "logs an error unless the ArrayList implements compact" do
        expect(log_receiver).not_to receive_log_message("Unhandled exception", key: :method, match: "send_to_riemann")
        # expect(log_receiver).not_to receive_log_message("Unhandled exception", key: :error, match: %r|undefined method .compact. for ..Java..JavaUtil..ArrayList|)
        # expect { output.receive(event) }.not_to receive_log_message("Unhandled exception", key: :error, match: %r|undefined method .compact. for ..Java..JavaUtil..ArrayList|)
      end
    end
  end
end
