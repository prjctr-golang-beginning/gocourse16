package proto

import (
	"fmt"
	"go.opentelemetry.io/otel/trace"
	"gocourse16/app/clickhouse/tcp/binary"
	"os"
)

var (
	osUser      = os.Getenv("USER")
	hostname, _ = os.Hostname()
)

type Query struct {
	ID             string
	Span           trace.SpanContext
	Body           string
	QuotaKey       string
	Settings       Settings
	Compression    bool
	InitialUser    string
	InitialAddress string
}

func (q *Query) Decode(decoder *binary.Decoder, revision uint64) error {
	var err error

	if q.ID, err = decoder.String(); err != nil {
		return err
	}
	// client_info
	if err := q.decodeClientInfo(decoder, revision); err != nil {
		return err
	}
	// settings
	for {
		set := Setting{}
		if err := set.decode(decoder, revision); err != nil {
			return err
		}
		if set.EmptySetting {
			break
		}
		q.Settings = append(q.Settings, set)
	}

	if revision >= DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET {
		_, _ = decoder.String() // ""
	}
	{
		if _, err = decoder.ReadByte(); err != nil { // StateComplete const
			return err
		}
		if q.Compression, err = decoder.Bool(); err != nil {
			return err
		}
	}

	if q.Body, err = decoder.String(); err != nil { // StateComplete const
		return err
	}

	return err
}

func (q *Query) decodeClientInfo(decoder *binary.Decoder, revision uint64) error {
	var err error

	_, _ = decoder.ReadByte()                // ClientQueryInitial const
	q.InitialUser, err = decoder.String()    // initial_user
	_, err = decoder.String()                // initial_query_id // ""
	q.InitialAddress, err = decoder.String() // initial_address
	if revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_INITIAL_QUERY_START_TIME {
		_, _ = decoder.Int64() // initial_query_start_time_microseconds // 0
	}
	_, _ = decoder.ReadByte() // interface [tcp - 1, http - 2] // 1
	{
		osUser, err = decoder.String()
		hostname, err = decoder.String()
		_, err = decoder.String()  // ClientName
		_, err = decoder.Uvarint() // ClientVersionMajor
		_, err = decoder.Uvarint() // ClientVersionMinor
		_, err = decoder.Uvarint() // ClientTCPProtocolVersion
	}
	if revision >= DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO {
		q.QuotaKey, err = decoder.String()
	}
	if revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_DISTRIBUTED_DEPTH {
		_, _ = decoder.Uvarint() // 0
	}
	if revision >= DBMS_MIN_REVISION_WITH_VERSION_PATCH {
		_, _ = decoder.Uvarint() // 0
	}
	if revision >= DBMS_MIN_REVISION_WITH_OPENTELEMETRY {
		switch {
		case q.Span.IsValid():
			_, _ = decoder.ReadByte() // 1
			{
				v := q.Span.TraceID()
				_ = decoder.Raw(v[:])
			}
			{
				v := q.Span.SpanID()
				_ = decoder.Raw(v[:])
			}
			_, _ = decoder.String()   // q.Span.TraceState().String()
			_, _ = decoder.ReadByte() // byte(q.Span.TraceFlags())

		default:
			_, _ = decoder.ReadByte() // 0
		}
	}
	if revision >= DBMS_MIN_REVISION_WITH_PARALLEL_REPLICAS {
		_, _ = decoder.Uvarint() // collaborate_with_initiator // 0
		_, _ = decoder.Uvarint() // count_participating_replicas // 0
		_, _ = decoder.Uvarint() // number_of_current_replica // 0
	}

	return err
}

func (q *Query) Encode(encoder *binary.Encoder, revision uint64) error {
	if err := encoder.String(q.ID); err != nil {
		return err
	}
	// client_info
	if err := q.encodeClientInfo(encoder, revision); err != nil {
		return err
	}
	// settings
	if err := q.Settings.Encode(encoder, revision); err != nil {
		return err
	}
	encoder.String("" /* empty string is a marker of the end of setting */)

	if revision >= DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET {
		encoder.String("")
	}
	{
		encoder.Byte(StateComplete)
		encoder.Bool(q.Compression)
	}
	return encoder.String(q.Body)
}

func (q *Query) encodeClientInfo(encoder *binary.Encoder, revision uint64) error {
	encoder.Byte(ClientQueryInitial)
	encoder.String(q.InitialUser)    // initial_user
	encoder.String("")               // initial_query_id
	encoder.String(q.InitialAddress) // initial_address
	if revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_INITIAL_QUERY_START_TIME {
		encoder.Int64(0) // initial_query_start_time_microseconds
	}
	encoder.Byte(1) // interface [tcp - 1, http - 2]
	{
		encoder.String(osUser)
		encoder.String(hostname)
		encoder.String("reverse proxy") // ClientName
		encoder.Uvarint(ClientVersionMajor)
		encoder.Uvarint(ClientVersionMinor)
		encoder.Uvarint(ClientTCPProtocolVersion)
	}
	if revision >= DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO {
		encoder.String(q.QuotaKey)
	}
	if revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_DISTRIBUTED_DEPTH {
		encoder.Uvarint(0)
	}
	if revision >= DBMS_MIN_REVISION_WITH_VERSION_PATCH {
		encoder.Uvarint(0)
	}
	if revision >= DBMS_MIN_REVISION_WITH_OPENTELEMETRY {
		switch {
		case q.Span.IsValid():
			encoder.Byte(1)
			{
				v := q.Span.TraceID()
				encoder.Raw(v[:])
			}
			{
				v := q.Span.SpanID()
				encoder.Raw(v[:])
			}
			encoder.String(q.Span.TraceState().String())
			encoder.Byte(byte(q.Span.TraceFlags()))

		default:
			encoder.Byte(0)
		}
	}
	if revision >= DBMS_MIN_REVISION_WITH_PARALLEL_REPLICAS {
		encoder.Uvarint(0) // collaborate_with_initiator
		encoder.Uvarint(0) // count_participating_replicas
		encoder.Uvarint(0) // number_of_current_replica
	}
	return nil
}

type Settings []Setting

type Setting struct {
	Key          string
	Value        interface{}
	EmptySetting bool
}

func (s *Setting) decode(decoder *binary.Decoder, revision uint64) error {
	var err error
	if s.Key, err = decoder.String(); err != nil {
		return err
	}
	if s.Key == "" { /* empty string is a marker of the end of setting */
		s.EmptySetting = true
		return nil
	}
	if revision <= DBMS_MIN_REVISION_WITH_SETTINGS_SERIALIZED_AS_STRINGS {
		//var value uint64
		//switch v := s.Value.(type) {
		//case int:
		//	value = uint64(v)
		//case bool:
		//	if value = 0; v {
		//		value = 1
		//	}
		//default:
		//	return fmt.Errorf("query setting %s has unsupported data type", s.Key)
		//}
		if _, err = decoder.Uvarint(); err != nil { // is_important // value
			return err
		}
	}
	if _, err := decoder.Bool(); err != nil { // is_important // true
		return err
	}
	if s.Value, err = decoder.String(); err != nil { // is_important
		return err
	}

	return err
}

func (s Settings) Encode(encoder *binary.Encoder, revision uint64) error {
	for _, s := range s {
		if err := s.encode(encoder, revision); err != nil {
			return err
		}
	}
	return nil
}

func (s *Setting) encode(encoder *binary.Encoder, revision uint64) error {
	if err := encoder.String(s.Key); err != nil {
		return err
	}
	if revision <= DBMS_MIN_REVISION_WITH_SETTINGS_SERIALIZED_AS_STRINGS {
		var value uint64
		switch v := s.Value.(type) {
		case int:
			value = uint64(v)
		case bool:
			if value = 0; v {
				value = 1
			}
		default:
			return fmt.Errorf("query setting %s has unsupported data type", s.Key)
		}
		return encoder.Uvarint(value)
	}
	if err := encoder.Bool(true); err != nil { // is_important
		return err
	}
	return encoder.String(fmt.Sprint(s.Value))
}
