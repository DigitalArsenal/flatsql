// Reference projection: extractIndexedFields + recordIndexArgs copied
// verbatim from sdn-server/internal/storage/flatsql.go (published SDS Go
// bindings v1.226.0). Reads concatenated size-prefixed frames, prints one
// JSON line per record: [norad, entity, objectType, opsStatus, epoch, day,
// supersede identity] (recordSupersedeKey from record_supersede.go).
package main

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/DigitalArsenal/spacedatastandards.org/lib/go/CAT"
	"github.com/DigitalArsenal/spacedatastandards.org/lib/go/MPE"
	"github.com/DigitalArsenal/spacedatastandards.org/lib/go/OEM"
	"github.com/DigitalArsenal/spacedatastandards.org/lib/go/OMM"
	"github.com/DigitalArsenal/spacedatastandards.org/lib/go/PNM"
	"github.com/DigitalArsenal/spacedatastandards.org/lib/go/RFB"
	flatbuffers "github.com/google/flatbuffers/go"
)

func main() {
	schema := os.Args[1]
	data, err := os.ReadFile(os.Args[2])
	if err != nil {
		panic(err)
	}
	w := bufio.NewWriter(os.Stdout)
	defer w.Flush()
	for off := 0; off < len(data); {
		n := int(binary.LittleEndian.Uint32(data[off:]))
		frame := data[off : off+4+n]
		off += 4 + n
		args := recordIndexArgs(schema, "", 0, frame)
		row := append(args[2:8:8], recordSupersedeKey(schema, frame))
		b, _ := json.Marshal(row)
		fmt.Fprintln(w, string(b))
	}
}

type indexedFields struct {
	noradCatID    *uint32
	entityID      string
	objectType    string
	opsStatusCode string
	epochUnix     *int64
	epochDay      string
}

func recordIndexArgs(schemaName, cid string, sourceTimestamp int64, data []byte) []any {
	fields, err := extractIndexedFields(schemaName, data)
	if err != nil {
		// The index is the global record catalog + sync cursor (WS7.3d): every
		// stored record MUST get a row so no record is invisible to a
		// rowid-cursor scan. A field-extraction failure (e.g. an unparseable
		// OMM/CAT payload) still gets a bare index row with only the source
		// timestamp; the structured columns stay NULL.
		fields = &indexedFields{}
	}

	var norad interface{}
	if fields.noradCatID != nil {
		norad = int64(*fields.noradCatID)
	}
	var entity interface{}
	if fields.entityID != "" {
		entity = fields.entityID
	}
	var objectType interface{}
	if fields.objectType != "" {
		objectType = fields.objectType
	}
	var opsStatusCode interface{}
	if fields.opsStatusCode != "" {
		opsStatusCode = fields.opsStatusCode
	}
	var epoch interface{}
	if fields.epochUnix != nil {
		epoch = *fields.epochUnix
	}
	var day interface{}
	if fields.epochDay != "" {
		day = fields.epochDay
	}
	return []any{schemaName, cid, norad, entity, objectType, opsStatusCode, epoch, day, sourceTimestamp}
}

func extractIndexedFields(schemaName string, data []byte) (*indexedFields, error) {
	out := &indexedFields{}

	switch schemaName {
	case "OMM.fbs":
		omm, err := parseOMM(data)
		if err != nil {
			return nil, err
		}
		if id := omm.NORAD_CAT_ID(); id > 0 {
			idCopy := id
			out.noradCatID = &idCopy
		}
		out.entityID = strings.TrimSpace(string(omm.OBJECT_ID()))

		epochStr := strings.TrimSpace(string(omm.EPOCH()))
		if epochStr == "" {
			epochStr = strings.TrimSpace(string(omm.CREATION_DATE()))
		}
		if epochStr != "" {
			epochUnix, err := parseEpochString(epochStr)
			if err == nil {
				out.epochUnix = &epochUnix
				out.epochDay = time.Unix(epochUnix, 0).UTC().Format("2006-01-02")
			}
		}
	case "MPE.fbs":
		mpe, err := parseMPE(data)
		if err != nil {
			return nil, err
		}
		out.entityID = strings.TrimSpace(string(mpe.ENTITY_ID()))
		if epochValue := mpe.EPOCH(); epochValue != 0 {
			// Go's float-to-int conversion truncates toward zero. Floor first so
			// a fractional pre-1970 epoch remains on its correct UTC day.
			epoch := int64(math.Floor(epochValue))
			out.epochUnix = &epoch
			out.epochDay = time.Unix(epoch, 0).UTC().Format("2006-01-02")
		}

	case "OEM.fbs":
		oem, err := parseOEM(data)
		if err != nil {
			return nil, err
		}
		block, ok := firstOEMDataBlock(oem)
		if !ok {
			return out, nil
		}
		objectOffset := flatbuffers.UOffsetT(block.Offset(6))
		if objectOffset == 0 {
			// An OEM block without OBJECT has no stable object identity, so it
			// contributes no structured index fields.
			return out, nil
		}
		object := &OEM.CAT{}
		object.Init(block.Bytes, block.Indirect(objectOffset+block.Pos))
		out.entityID = strings.TrimSpace(string(object.OBJECT_ID()))
		if id := object.NORAD_CAT_ID(); id > 0 {
			idCopy := id
			out.noradCatID = &idCopy
		}

		epochStr := flatBufferTableString(block, 18)
		if epochStr == "" {
			epochStr = firstOEMDataLineEpoch(block)
		}
		if epochStr != "" {
			if epochUnix, parseErr := parseEpochString(epochStr); parseErr == nil {
				out.epochUnix = &epochUnix
				out.epochDay = time.Unix(epochUnix, 0).UTC().Format("2006-01-02")
			}
		}

	case "CAT.fbs":
		cat, err := parseCAT(data)
		if err != nil {
			return nil, err
		}
		if id := cat.NORAD_CAT_ID(); id > 0 {
			idCopy := id
			out.noradCatID = &idCopy
		}
		// The international designator is the object's identity when it has
		// no NORAD number (un-numbered launches, analyst objects); without it
		// such a record was invisible to every ?entity= lookup.
		out.entityID = strings.TrimSpace(string(cat.OBJECT_ID()))
		if objectType := strings.TrimSpace(cat.OBJECT_TYPE().String()); objectType != "" && objectType != "UNKNOWN" {
			out.objectType = objectType
		}
		if opsStatusCode := strings.TrimSpace(cat.OPS_STATUS_CODE().String()); opsStatusCode != "" && opsStatusCode != "UNKNOWN" {
			out.opsStatusCode = opsStatusCode
		}

	case "PNM.fbs":
		pnm, err := parsePNM(data)
		if err != nil {
			return nil, err
		}
		out.entityID = strings.TrimSpace(string(pnm.FILE_ID()))

	case "RFB.fbs":
		// RF emitters are NORAD-keyed data (sdn-data-index-rfb-norad). Without
		// this case every $RFB row indexed with norad_cat_id NULL even though
		// the bytes carried NORAD_CAT_ID, so /api/v1/data/index answered
		// norad:null for all 5,289 SatNOGS records and ?norad= matched nothing —
		// the whole "which satellites transmit on S-band" query path.
		//
		// entity_id is the TRANSMITTER, not the spacecraft: one RFB record
		// describes exactly one emission of one device, and an UPLINK/DOWNLINK
		// pair shares ID_TRANSMITTER (RFB.fbs). The spacecraft is already the
		// norad_cat_id column, so indexing ID_ENTITY here would duplicate it and
		// lose the ability to find the two halves of one transponder. ID is the
		// fallback for a record with no device identifier.
		//
		// No epoch: $RFB is a specification of a band, not an observation at a
		// time. Leaving epoch_unix NULL is the honest projection — a synthesized
		// ingest time would make every emitter look like it changed today.
		rfb, err := parseRFB(data)
		if err != nil {
			return nil, err
		}
		if id := rfb.NORAD_CAT_ID(); id > 0 {
			idCopy := id
			out.noradCatID = &idCopy
		}
		out.entityID = strings.TrimSpace(string(rfb.ID_TRANSMITTER()))
		if out.entityID == "" {
			out.entityID = strings.TrimSpace(string(rfb.ID()))
		}

	default:
		// No structured extraction for this schema yet.
	}

	return out, nil
}

func parseOMM(data []byte) (omm *OMM.OMM, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("malformed OMM buffer: %v", r)
		}
	}()
	switch {
	case OMM.SizePrefixedOMMBufferHasIdentifier(data):
		return OMM.GetSizePrefixedRootAsOMM(data, 0), nil
	case OMM.OMMBufferHasIdentifier(data):
		return OMM.GetRootAsOMM(data, 0), nil
	default:
		return nil, errors.New("invalid OMM buffer")
	}
}

func parsePNM(data []byte) (pnm *PNM.PNM, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("malformed PNM buffer: %v", r)
		}
	}()
	switch {
	case PNM.SizePrefixedPNMBufferHasIdentifier(data):
		return PNM.GetSizePrefixedRootAsPNM(data, 0), nil
	case PNM.PNMBufferHasIdentifier(data):
		return PNM.GetRootAsPNM(data, 0), nil
	default:
		return nil, errors.New("invalid PNM buffer")
	}
}

func parseRFB(data []byte) (rfb *RFB.RFB, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("malformed RFB buffer: %v", r)
		}
	}()
	switch {
	case RFB.SizePrefixedRFBBufferHasIdentifier(data):
		return RFB.GetSizePrefixedRootAsRFB(data, 0), nil
	case RFB.RFBBufferHasIdentifier(data):
		return RFB.GetRootAsRFB(data, 0), nil
	default:
		return nil, errors.New("invalid RFB buffer")
	}
}

func parseMPE(data []byte) (mpe *MPE.MPE, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("malformed MPE buffer: %v", r)
		}
	}()
	switch {
	case MPE.SizePrefixedMPEBufferHasIdentifier(data):
		return MPE.GetSizePrefixedRootAsMPE(data, 0), nil
	case MPE.MPEBufferHasIdentifier(data):
		return MPE.GetRootAsMPE(data, 0), nil
	default:
		return nil, errors.New("invalid MPE buffer")
	}
}

func parseOEM(data []byte) (oem *OEM.OEM, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("malformed OEM buffer: %v", r)
		}
	}()
	switch {
	case OEM.SizePrefixedOEMBufferHasIdentifier(data):
		return OEM.GetSizePrefixedRootAsOEM(data, 0), nil
	case OEM.OEMBufferHasIdentifier(data):
		return OEM.GetRootAsOEM(data, 0), nil
	default:
		return nil, errors.New("invalid OEM buffer")
	}
}

func firstOEMDataBlock(oem *OEM.OEM) (flatbuffers.Table, bool) {
	root := oem.Table()
	offset := flatbuffers.UOffsetT(root.Offset(12))
	if offset == 0 || root.VectorLen(offset) == 0 {
		return flatbuffers.Table{}, false
	}
	return flatbuffers.Table{Bytes: root.Bytes, Pos: root.Indirect(root.Vector(offset))}, true
}

func flatBufferTableString(table flatbuffers.Table, vtableOffset flatbuffers.VOffsetT) string {
	offset := flatbuffers.UOffsetT(table.Offset(vtableOffset))
	if offset == 0 {
		return ""
	}
	return strings.TrimSpace(string(table.ByteVector(offset + table.Pos)))
}

func firstOEMDataLineEpoch(block flatbuffers.Table) string {
	offset := flatbuffers.UOffsetT(block.Offset(36))
	if offset == 0 || block.VectorLen(offset) == 0 {
		return ""
	}
	line := flatbuffers.Table{Bytes: block.Bytes, Pos: block.Indirect(block.Vector(offset))}
	return flatBufferTableString(line, 4)
}

func parseCAT(data []byte) (cat *CAT.CAT, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("malformed CAT buffer: %v", r)
		}
	}()
	switch {
	case CAT.SizePrefixedCATBufferHasIdentifier(data):
		return CAT.GetSizePrefixedRootAsCAT(data, 0), nil
	case CAT.CATBufferHasIdentifier(data):
		return CAT.GetRootAsCAT(data, 0), nil
	default:
		return nil, errors.New("invalid CAT buffer")
	}
}

func parseEpochString(raw string) (int64, error) {
	normalized := strings.TrimSpace(raw)
	if normalized == "" {
		return 0, errors.New("empty epoch")
	}

	layouts := []string{
		time.RFC3339Nano,
		"2006-01-02T15:04:05.000000",
		"2006-01-02T15:04:05.000",
		"2006-01-02T15:04:05",
		"2006-01-02 15:04:05",
		"2006-01-02",
	}

	for _, layout := range layouts {
		if t, err := time.Parse(layout, normalized); err == nil {
			return t.UTC().Unix(), nil
		}
	}

	if floatEpoch, err := strconv.ParseFloat(normalized, 64); err == nil && floatEpoch > 0 {
		return int64(floatEpoch), nil
	}

	return 0, fmt.Errorf("unsupported epoch format: %q", raw)
}


const catCatalogURISlot      = 52
const catCatalogObjectIDSlot = 54

func recordSupersedeKey(schemaName string, data []byte) string {
	if schemaName != "CAT.fbs" {
		return ""
	}
	cat, err := parseCAT(data)
	if err != nil {
		return ""
	}
	tab := cat.Table()
	catalogURI := strings.TrimSpace(string(tableStringField(tab, catCatalogURISlot)))
	catalogObjectID := strings.TrimSpace(string(tableStringField(tab, catCatalogObjectIDSlot)))
	if catalogURI != "" && catalogObjectID != "" {
		return "uri:" + catalogURI + "\x00" + catalogObjectID
	}
	if id := cat.NORAD_CAT_ID(); id > 0 {
		return "norad:" + strconv.FormatUint(uint64(id), 10)
	}
	if objectID := strings.TrimSpace(string(cat.OBJECT_ID())); objectID != "" {
		return "object:" + objectID
	}
	return ""
}

func tableStringField(tab flatbuffers.Table, slot flatbuffers.VOffsetT) []byte {
	o := flatbuffers.UOffsetT(tab.Offset(slot))
	if o == 0 {
		return nil
	}
	return tab.ByteVector(o + tab.Pos)
}
