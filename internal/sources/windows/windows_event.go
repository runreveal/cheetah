package windows

import (
	"bytes"
	"encoding/xml"
	"io"
	"time"
)

type xmlMap map[string]interface{}

type xmlMapEntry struct {
	XMLName  xml.Name
	Value    string `xml:",chardata"`
	InnerXML string `xml:",innerxml"`
}

func (m *xmlMap) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	*m = xmlMap{}
	for {
		var e xmlMapEntry

		err := d.Decode(&e)
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		if e.InnerXML != "" {
			var sm xmlMap
			r := bytes.NewBuffer([]byte(e.InnerXML))
			dec := xml.NewDecoder(r)
			err := sm.UnmarshalXML(dec, xml.StartElement{})

			if err == io.EOF {
				break
			} else if err != nil {
				return err
			}
			(*m)[e.XMLName.Local] = sm
		}
		if e.Value != "" {
			(*m)[e.XMLName.Local] = e.Value
		}
	}
	return nil
}

type Data struct {
	Name  string `xml:"Name,attr"`
	Value string `xml:",innerxml"`
} //`xml:"Data"`

type xmlEvent struct {
	// seems to always have the same format
	// if not consider using XMLMap
	EventData struct {
		Data []Data
	} `xml:"EventData,omitempty"`
	// Using XMLMap type because we don't know what is inside (a priori)
	UserData xmlMap
	System   struct {
		Provider struct {
			Name string `xml:"Name,attr"`
			Guid string `xml:"Guid,attr"`
		} `xml:"Provider"`
		EventID     string `xml:"EventID"`
		Version     string `xml:"Version"`
		Level       string `xml:"Level"`
		Task        string `xml:"Task"`
		Opcode      string `xml:"Opcode"`
		Keywords    string `xml:"Keywords"`
		TimeCreated struct {
			SystemTime time.Time `xml:"SystemTime,attr"`
		} `xml:"TimeCreated"`
		EventRecordID string `xml:"EventRecordID"`
		Correlation   struct {
		} `xml:"Correlation"`
		Execution struct {
			ProcessID string `xml:"ProcessID,attr"`
			ThreadID  string `xml:"ThreadID,attr"`
		} `xml:"Execution"`
		Channel  string `xml:"Channel"`
		Computer string `xml:"Computer"`
		Security struct {
			UserID string `xml:"UserID,attr"`
		} `xml:"Security"`
	} `xml:"System"`
}

// ToMap converts an XMLEvent to an accurate structure to be serialized
// where EventData / UserData does not appear if empty
func (xe *xmlEvent) ToMap() *map[string]interface{} {
	m := make(map[string]interface{})
	m["Event"] = make(map[string]interface{})
	if len(xe.EventData.Data) > 0 {
		m["Event"].(map[string]interface{})["EventData"] = make(map[string]interface{})
		for _, d := range xe.EventData.Data {
			m["Event"].(map[string]interface{})["EventData"].(map[string]interface{})[d.Name] = d.Value
		}
	}
	if len(xe.UserData) > 0 {
		m["Event"].(map[string]interface{})["UserData"] = xe.UserData
	}
	m["Event"].(map[string]interface{})["System"] = xe.System
	return &m
}

func (xe *xmlEvent) ToJSONEvent() *jsonEvent {
	je := newJSONEvent()
	for _, d := range xe.EventData.Data {
		if d.Name != "" {
			je.Event.EventDataMap[d.Name] = d.Value
		} else {
			je.Event.EventData = append(je.Event.EventData, d.Value)
		}
	}
	je.Event.UserData = xe.UserData
	// System
	je.Event.System.Provider.Name = xe.System.Provider.Name
	je.Event.System.Provider.Guid = xe.System.Provider.Guid
	je.Event.System.EventID = xe.System.EventID
	je.Event.System.Version = xe.System.Version
	je.Event.System.Level = xe.System.Level
	je.Event.System.Task = xe.System.Task
	je.Event.System.Opcode = xe.System.Opcode
	je.Event.System.Keywords = xe.System.Keywords
	je.Event.System.TimeCreated.SystemTime = xe.System.TimeCreated.SystemTime
	je.Event.System.EventRecordID = xe.System.EventRecordID
	je.Event.System.Correlation = xe.System.Correlation
	je.Event.System.Execution.ProcessID = xe.System.Execution.ProcessID
	je.Event.System.Execution.ThreadID = xe.System.Execution.ThreadID
	je.Event.System.Channel = xe.System.Channel
	je.Event.System.Computer = xe.System.Computer
	je.Event.System.Security.UserID = xe.System.Security.UserID
	return &je
}

type jsonEvent struct {
	Event struct {
		EventDataMap map[string]string      `xml:"EventData" json:"eventDataMap,omitempty"`
		EventData    []string               `json:"eventData,omitempty"`
		UserData     map[string]interface{} `json:"userData,omitempty"`
		System       struct {
			Provider struct {
				Name string `xml:"Name,attr" json:"name"`
				Guid string `xml:"Guid,attr" json:"guid"`
			} `xml:"Provider" json:"provider"`
			EventID     string `xml:"EventID" json:"eventId"`
			Version     string `xml:"Version" json:"version"`
			Level       string `xml:"Level" json:"level"`
			Task        string `xml:"Task" json:"task"`
			Opcode      string `xml:"Opcode" json:"opcode"`
			Keywords    string `xml:"Keywords" json:"keywords"`
			TimeCreated struct {
				SystemTime time.Time `xml:"SystemTime,attr" json:"systemTime"`
			} `xml:"TimeCreated" json:"timeCreated"`
			EventRecordID string `xml:"EventRecordID" json:"eventRecordId"`
			Correlation   struct {
			} `xml:"Correlation" json:"correlation"`
			Execution struct {
				ProcessID string `xml:"ProcessID,attr" json:"processId"`
				ThreadID  string `xml:"ThreadID,attr" json:"threadId"`
			} `xml:"Execution" json:"execution"`
			Channel  string `xml:"Channel" json:"channel"`
			Computer string `xml:"Computer" json:"computer"`
			Security struct {
				UserID string `xml:"UserID,attr" json:"userId"`
			} `xml:"Security" json:"security"`
		} `xml:"System" json:"system"`
	}
}

// NewJSONEvent creates a new JSONEvent structure
func newJSONEvent() (je jsonEvent) {
	je.Event.EventDataMap = make(map[string]string)
	return je
}
