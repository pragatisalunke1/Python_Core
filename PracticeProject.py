#!/usr/bin/env python
# coding: utf-8

# In[15]:


def get_event_date(event):
  return event.date

def current_users(events):
  events.sort(key=get_event_date)
  machines = {}
  for event in events:
    if event.machine not in machines:
      machines[event.machine] = set()
    if event.type == "login":
      machines[event.machine].add(event.user)
    elif event.type == "logout"and event.user in machines[event.machine]:
      machines[event.machine].remove(event.user)
  return machines

def generate_report(machines):
  for machine, users in machines.items():
    if len(users) > 0:
      user_list = ", ".join(users)
      print("{}: {}".format(machine, user_list))


# In[16]:


class Event:
  def __init__(self, event_date, event_type, machine_name, user):
    self.date = event_date
    self.type = event_type
    self.machine = machine_name
    self.user = user


# In[17]:


events = [
    Event('2020-01-21 12:45:56', 'login', 'myworkstation.local', 'Pragati'),
    Event('2020-01-22 15:53:42', 'logout', 'webserver.local', 'Pragati'),
    Event('2020-01-21 18:53:21', 'login', 'webserver.local', 'Vikas'),
    Event('2020-01-22 10:25:34', 'logout', 'myworkstation.local', 'Pragati'),
    Event('2020-01-21 08:20:01', 'login', 'webserver.local', 'Pragati'),
    Event('2020-01-23 11:24:35', 'logout', 'mailserver.local', 'Vikas'),
]


# In[18]:


users = current_users(events)
print(users)


# In[19]:


generate_report(users)


# In[ ]:




