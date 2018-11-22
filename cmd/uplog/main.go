package main

import (
	"bytes"
	"context"
	"encoding/base32"
	"flag"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/compute/metadata"
	logging "cloud.google.com/go/logging/apiv2"
	"github.com/coreos/go-systemd/sdjournal"
	"github.com/golang/glog"
	"github.com/golang/protobuf/proto"
	_struct "github.com/golang/protobuf/ptypes/struct"
	timestamp "github.com/golang/protobuf/ptypes/timestamp"
	jsoniter "github.com/json-iterator/go"
	mrpb "google.golang.org/genproto/googleapis/api/monitoredres"
	logtypepb "google.golang.org/genproto/googleapis/logging/type"
	logpb "google.golang.org/genproto/googleapis/logging/v2"

	"k8s.io/uplog/pkg/uplog"
)

var (
	// TODO: Which characters are actually allowed?  Can we get to base64?
	// Stackdriver allows a-z and 0-9
	stackdriverInsertIdEncoding = base32.NewEncoding("ABCDEFGHIJKLMNOPQRSTUVWXYZ012345").WithPadding(base32.NoPadding)
)

func main() {
	flag.Set("logtostderr", "true")
	flag.Parse()

	err := runDockerLog()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
}

type dockerLogLine struct {
	Log    string `json:"log"`
	Stream string `json:"stream"`
	Time   string `json:"time"`
}

type PipelineContext struct {
}

func run() error {
	ctx := context.Background()
	l, err := logging.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("error building logging client: %v", err)
	}

	if !metadata.OnGCE() {
		glog.Fatalf("not running on GCE")
	}

	instanceProjectID, err := metadata.ProjectID()
	if err != nil {
		return fmt.Errorf("error fetching project id: %v", err)
	}

	instanceID, err := metadata.InstanceID()
	if err != nil {
		return fmt.Errorf("error fetching instance id: %v", err)
	}

	zone, err := metadata.Zone()
	if err != nil {
		return fmt.Errorf("error fetching zone: %v", err)
	}

	reportProjectID := instanceProjectID
	logID := "uplog" // must be url encoded
	logName := "projects/" + reportProjectID + "/logs/" + logID

	resource := &mrpb.MonitoredResource{
		Type: "gce_instance",
		Labels: map[string]string{
			"instance_id": instanceID,
			"project_id":  instanceProjectID,
			"zone":        zone,
		},
	}

	glog.Infof("resource is %+v", resource)
	time.Sleep(5 * time.Second)

	// j, err := sdjournal.NewJournal()
	j, err := sdjournal.NewJournalFromDir("/var/log/journal")
	if err != nil {
		return fmt.Errorf("error opening journal: %v", err)
	}
	defer j.Close()

	start := time.Now().Add(-15 * time.Minute)
	if err := j.SeekRealtimeUsec(uint64(start.UnixNano() / 1000)); err != nil {
		return fmt.Errorf("error seeking: %v", err)
	}

	req := &logpb.WriteLogEntriesRequest{}

	for {
		n, err := j.Next()
		if err != nil {
			return fmt.Errorf("error reading from journal: %s", err)
		}

		if n == 0 {
			// EOF

			if len(req.Entries) != 0 {
				if err := sendMessage(ctx, l, req); err != nil {
					glog.Warningf("failed to send message: %v", err)
				} else {
					req.Entries = nil
				}
			}

		waitForMessage:
			for {
				status := j.Wait(1000 * time.Millisecond)
				switch status {
				case sdjournal.SD_JOURNAL_NOP:
					// No new events
					break
				case sdjournal.SD_JOURNAL_APPEND:
					break waitForMessage
				case sdjournal.SD_JOURNAL_INVALIDATE:
					//https://www.freedesktop.org/software/systemd/man/sd_journal_get_fd.html:
					// "Programs only interested in a strictly sequential stream of log data may treat SD_JOURNAL_INVALIDATE the same way as SD_JOURNAL_APPEND, thus ignoring any changes to the log view earlier than the old end of the log stream."
					break waitForMessage
				default:
					return fmt.Errorf("unknown status from waiting for journal: %d", status)

				}
			}

			continue
		}

		// TODO: This function is overkill for our needs, and is not memory-bounded.
		// We should likely implement our own version
		entry, err := j.GetEntry()
		if err != nil {
			return fmt.Errorf("error getting entry: %v", err)
		}

		fmt.Printf("entry: %+v\n\n", entry)

		//body := entry.Fields[sdjournal.SD_JOURNAL_FIELD_MESSAGE]

		jsonPayload := &_struct.Struct{
			Fields: make(map[string]*_struct.Value),
		}

		for k, v := range entry.Fields {
			jsonPayload.Fields[k] = &_struct.Value{
				Kind: &_struct.Value_StringValue{StringValue: v},
			}
		}

		t := &timestamp.Timestamp{
			Seconds: int64(entry.RealtimeTimestamp / 1000000),
			Nanos:   int32((entry.RealtimeTimestamp % 1000000) * 1000),
		}

		logEntry := &logpb.LogEntry{
			LogName:  logName,
			Resource: resource,
			//Payload:  &logpb.LogEntry_TextPayload{TextPayload: body},
			Payload:   &logpb.LogEntry_JsonPayload{JsonPayload: jsonPayload},
			Timestamp: t,
		}
		req.Entries = append(req.Entries, logEntry)

		if len(req.Entries) >= 1000 {
			if err := sendMessage(ctx, l, req); err != nil {
				glog.Warningf("unable to write messages: %v", err)
			} else {
				req.Entries = nil
			}
		}
	}

	return nil
}

func runDockerLog() error {
	ctx := &UplogContext{
		Context: context.TODO(),
	}

	/* Json Log Example:
	       # {"log":"[info:2016-02-16T16:04:05.930-08:00] Some log text here\n","stream":"stdout","time":"2016-02-17T00:04:05.931087621Z"}
	           # CRI Log Example:
	   	    # 2016-02-17T00:04:05.931087621Z stdout F [info:2016-02-16T16:04:05.930-08:00] Some log text here
	*/

	dir := ctx.NewDirectoryScanner("/var/log/containers")

	return dir.Run()
}

type DirectoryScanner struct {
	ctx     *UplogContext
	basedir string
	readers map[string]*FileLineReader
}

func (c *UplogContext) NewDirectoryScanner(basedir string) *DirectoryScanner {
	d := &DirectoryScanner{
		ctx:     c,
		readers: make(map[string]*FileLineReader),
		basedir: basedir,
	}
	return d
}

func (r *DirectoryScanner) Run() error {
	sd, err := r.ctx.BuildStackDriverSink()
	if err != nil {
		return err
	}

	builder := func(p string, f os.FileInfo) (*FileLineReader, error) {
		name := f.Name()
		fr, err := r.ctx.NewFileLineReader(p)
		if err != nil {
			return nil, err
		}

		dockerParser, err := r.ctx.BuildDockerParser(name)
		if err != nil {
			return nil, err
		}
		fr.Out = dockerParser

		dockerParser.Out = sd
		return fr, nil
	}

	refreshDirectoryInterval := 30
	n := 0

	buffer := &uplog.ByteBuffer{}

	for {
		// We could also refresh the directory every time and use the size to avoid re-reading files
		if (n % refreshDirectoryInterval) == 0 {
			if err := r.scanForFiles(r.basedir, builder); err != nil {
				glog.Warningf("error scanning directory: %v", err)
			}
		}

		for _, f := range r.readers {
			buffer.Clear()
			f.Poll(r.ctx, buffer)
		}

		// TODO: Should we flush all our readers?
		sd.Flush()

		time.Sleep(time.Second)
		n++
	}

	return nil
}

func (r *DirectoryScanner) scanForFiles(dir string, builder func(p string, fi os.FileInfo) (*FileLineReader, error)) error {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("unable to read dir %s: %v", dir, err)
	}

	for _, f := range files {
		name := f.Name()
		// Don't be startled by our own shadow (infinite loops)
		if strings.Contains(name, "uplog") {
			continue
		}

		p := filepath.Join(dir, name)

		if r.readers[p] == nil {
			fr, err := builder(p, f)
			if err != nil {
				glog.Warningf("error building reader for %s: %v", p, err)
				continue
			}
			r.readers[p] = fr
		}
	}
	return nil
}

type FileLineReader struct {
	f       *os.File
	Out     LineSink
	seq     string
	filePos int64
}

func (c *UplogContext) NewFileLineReader(p string) (*FileLineReader, error) {
	f, err := os.OpenFile(p, os.O_RDONLY, 0)
	if err != nil {
		return nil, fmt.Errorf("error opening file %s: %v", p, err)
	}

	var seq string
	{
		hasher := fnv.New64a()
		// TODO: Use container id?
		// TODO: Include file create time or inode
		key := fmt.Sprintf("%s", p)
		hasher.Write([]byte(key))

		seq = stackdriverInsertIdEncoding.EncodeToString(hasher.Sum(nil))
	}

	glog.Infof("tailing file %s (%s)", p, seq)

	// TODO: Save marker
	pos := int64(0)

	r := &FileLineReader{
		f:       f,
		seq:     seq,
		filePos: pos,
	}
	return r, nil
}

func (r *FileLineReader) Flush() {
	if r.Out != nil {
		r.Out.Flush()
	}
}

func (r *FileLineReader) Close() error {
	if r.f != nil {
		err := r.f.Close()
		if err != nil {
			return err
		}
		r.f = nil
	}
	return nil
}

func (r *FileLineReader) Poll(ctx *UplogContext, bb *uplog.ByteBuffer) {
	eof := false

	for {
		// Note: we need to offset by any available bytes, which are ones we previously read and rolled over
		// We can't just Clear() every time either - it would be inefficient, but more importantly it would break our buffer-growing logic
		_, err := bb.WriteFromReaderAt(r.f, r.filePos+int64(bb.Available()))
		if err != nil {
			if err == io.EOF {
				//glog.Infof("found eof")
				eof = true
			} else {
				glog.Warningf("unexpected error reading file %s: %v", r.f.Name(), err)

				// Treat like eof - exit the poll loop
				eof = true
			}
		}

		//glog.Infof("%s: read %d bytes", r.seq, n)

		readbuf := bb.PeekAll()

		for len(readbuf) > 0 {
			lineEnd := bytes.IndexByte(readbuf, '\n')
			if lineEnd < 0 {
				// No more complete lines
				break
			}

			r.Out.GotLine(ctx, readbuf[:lineEnd], r.seq, r.filePos)

			// we add 1 because we want to skip over the \n byte
			r.filePos += int64(lineEnd + 1)
			bb.Skip(lineEnd + 1)
			readbuf = readbuf[lineEnd+1:]
		}

		if eof {
			// TODO: Complete last line when no more contents coming...
			break
		}
	}
}

type DockerParser struct {
	LogName  string
	Resource *mrpb.MonitoredResource
	Out      RecordSink
}

type Flushable interface {
	Flush()
}

type LineSink interface {
	Flushable
	GotLine(ctx *UplogContext, line []byte, seq string, pos int64)
}

var _ LineSink = &DockerParser{}

type UplogContext struct {
	context.Context
}

func (s *DockerParser) GotLine(ctx *UplogContext, line []byte, seq string, pos int64) {
	// jsoniter doesn't tolerate empty lines

	// We would remove \r here, except that json tolerates it
	lineStart := 0
skipBlanks:
	for lineStart < len(line) {
		switch line[lineStart] {
		case ' ', '\r', '\t':
			lineStart++

		default:
			break skipBlanks
		}
	}

	// Skip blank lines
	if lineStart >= len(line) {
		glog.Infof("skipping blank line")
		return
	}

	if lineStart != 0 {
		line = line[lineStart:]
	}

	//glog.Infof("line %s@%d => %q", seq, pos, string(line))
	json := jsoniter.ConfigFastest

	var dockerLogLine dockerLogLine
	if err := json.Unmarshal(line, &dockerLogLine); err != nil {
		// TODO: A metric instead - or at least prevent us infinite looping here
		glog.Warningf("error parsing line %q %s@%d: %v", string(line), seq, pos, err)
		return
	}

	ts, err := time.Parse(time.RFC3339Nano, dockerLogLine.Time)
	if err != nil {
		glog.Warningf("cannot parse timestamp %q", dockerLogLine.Time)
		return
	}

	nanos := ts.UnixNano()

	t := &timestamp.Timestamp{
		Seconds: int64(nanos / 1000000000),
		Nanos:   int32(nanos % 1000000000),
	}

	text := dockerLogLine.Log
	text = strings.TrimRight(text, "\n")

	logEntry := &logpb.LogEntry{
		LogName:  s.LogName,
		Resource: s.Resource,
		Payload:  &logpb.LogEntry_TextPayload{TextPayload: text},
		//Payload:   &logpb.LogEntry_JsonPayload{JsonPayload: jsonPayload},
		Timestamp: t,
	}

	{
		// InsertId is combined with timestamp to provide idempotency

		/*
			// TODO: Just do our own base encoding?  (we could just do hex as it is so much easier!)
			hasher := fnv.New64a()
			fmt.Fprintf(hasher, "%d", pos)

			logEntry.InsertId = seq + stackdriverInsertIdEncoding.EncodeToString(hasher.Sum(nil))
		*/

		logEntry.InsertId = seq + strconv.FormatInt(pos, 16)

		//glog.Infof("position %d, insertId %s", pos, logEntry.InsertId)
	}

	switch dockerLogLine.Stream {
	case "stdout":
		logEntry.Severity = logtypepb.LogSeverity_INFO
	case "stderr":
		logEntry.Severity = logtypepb.LogSeverity_ERROR
	default:
		glog.Warningf("unknown docker stream: %s", dockerLogLine.Stream)
	}

	s.Out.GotRecord(ctx, logEntry)
	//fmt.Printf("%+v\n", &dockerLogLine)
}

func (s *DockerParser) Flush() {
	if s.Out != nil {
		s.Out.Flush()
	}
}

type StackDriverSink struct {
	ctx    *UplogContext
	Client *logging.Client

	request             *logpb.WriteLogEntriesRequest
	requestSizeEstimate int
}

type RecordSink interface {
	Flushable
	GotRecord(ctx *UplogContext, entry *logpb.LogEntry)
}

func (s *StackDriverSink) Flush() {
	if s.request != nil && len(s.request.Entries) > 0 {
		s.sendMessage()
	}
}

func (s *StackDriverSink) GotRecord(ctx *UplogContext, entry *logpb.LogEntry) {
	if s.request == nil {
		s.request = &logpb.WriteLogEntriesRequest{}
	}

	entry.Labels = map[string]string{
		"from": "uplog",
	}

	entrySize := proto.Size(entry)
	s.requestSizeEstimate += entrySize

	req := s.request
	req.Entries = append(req.Entries, entry)

	flushSize := 128 * 1024

	// StackDriver has a limit of 1000
	if len(req.Entries) >= 900 || s.requestSizeEstimate >= flushSize {
		s.sendMessage()
	}
}

func (s *StackDriverSink) sendMessage() {
	if err := sendMessage(s.ctx, s.Client, s.request); err != nil {
		glog.Warningf("unable to write messages: %v", err)
	} else {
		s.request.Entries = nil
		s.requestSizeEstimate = 0
	}
}

func sendMessage(ctx context.Context, client *logging.Client, req *logpb.WriteLogEntriesRequest) error {
	// TODO: Using GRPC is likely to be the memory long pole.  If we _only_ use GRPC & HTTP2 for this,
	// we should evaluate switching to JSON over HTTP1.1.
	glog.Infof("sending %d entries to stackdriver", len(req.Entries))
	resp, err := client.WriteLogEntries(ctx, req)
	if err != nil {
		return fmt.Errorf("error writing log entries: %v", err)
	}
	glog.V(4).Infof("response %+v", resp)
	return nil
}

func gotLine(line []byte) {
	//glog.Infof("line %q", string(line))
	json := jsoniter.ConfigFastest

	var dockerLogLine dockerLogLine
	if err := json.Unmarshal(line, &dockerLogLine); err != nil {
		// TODO: A metric instead - or at least prevent us infinite looping here
		glog.Warningf("error parsing line: %v", err)
	}

	fmt.Printf("%+v\n", &dockerLogLine)

}

func (c *UplogContext) BuildDockerParser(filename string) (*DockerParser, error) {
	//	name := "fluentd-gcp-v3.1.0-7khbt_kube-system_fluentd-gcp-48bf1cbf9364446ffa8dffd67846116a242ad433fa02866c57fb6e3021630f6b.log"

	tokens := strings.Split(filename, "_")
	if len(tokens) != 3 {
		return nil, fmt.Errorf("unexpected name: %s", filename)
	}

	podName := tokens[0]
	namespace := tokens[1]

	pos := strings.LastIndex(tokens[2], "-")
	if pos == -1 {
		return nil, fmt.Errorf("unexpected name (container): %s", filename)
	}
	containerName := tokens[2][:pos]

	projectID, err := metadata.ProjectID()
	if err != nil {
		return nil, fmt.Errorf("error fetching project id: %v", err)
	}

	instanceID, err := metadata.InstanceID()
	if err != nil {
		return nil, fmt.Errorf("error fetching instance id: %v", err)
	}

	zone, err := metadata.Zone()
	if err != nil {
		return nil, fmt.Errorf("error fetching zone: %v", err)
	}

	clusterName, err := metadata.Get("instance/attributes/cluster-name")
	if err != nil {
		return nil, fmt.Errorf("error fetching metadata 'instance/attributes/cluster-name': %v", err)
	}

	logID := containerName // must be url encoded
	logName := "projects/" + projectID + "/logs/" + logID

	resource := &mrpb.MonitoredResource{
		Type: "container",
		Labels: map[string]string{
			"cluster_name":   clusterName,
			"container_name": containerName,
			"instance_id":    instanceID,
			"namespace_id":   namespace,
			"pod_id":         podName,
			"project_id":     projectID,
			"zone":           zone,
		},
	}

	p := &DockerParser{
		LogName:  logName,
		Resource: resource,
	}

	return p, nil
}

func (c *UplogContext) BuildStackDriverSink() (*StackDriverSink, error) {
	l, err := logging.NewClient(c)
	if err != nil {
		return nil, fmt.Errorf("error building logging client: %v", err)
	}
	sink := &StackDriverSink{
		ctx:    c,
		Client: l,
	}
	return sink, nil
}
