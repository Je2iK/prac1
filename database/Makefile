CXX = g++
CXXFLAGS = -std=c++17 -g -Wall -Wextra -Iinclude -Ithird_party/json/include -pthread
OBJDIR = obj
SRCDIR = src
ADTDIR = adt

CONSOLE_SOURCES = $(SRCDIR)/main.cpp $(SRCDIR)/Database.cpp $(SRCDIR)/Schema.cpp $(SRCDIR)/Table.cpp
CONSOLE_OBJECTS = $(CONSOLE_SOURCES:$(SRCDIR)/%.cpp=$(OBJDIR)/%.o)

SERVER_SOURCES = $(SRCDIR)/server.cpp $(SRCDIR)/Database.cpp $(SRCDIR)/Schema.cpp $(SRCDIR)/Table.cpp
SERVER_OBJECTS = $(SERVER_SOURCES:$(SRCDIR)/%.cpp=$(OBJDIR)/%.o)

CLIENT_SOURCES = $(SRCDIR)/client.cpp
CLIENT_OBJECTS = $(CLIENT_SOURCES:$(SRCDIR)/%.cpp=$(OBJDIR)/%.o)

CONSOLE_TARGET = database
SERVER_TARGET = database-server
CLIENT_TARGET = database-client

all: $(CONSOLE_TARGET) $(SERVER_TARGET) $(CLIENT_TARGET)

$(CONSOLE_TARGET): $(CONSOLE_OBJECTS)
	$(CXX) $(CONSOLE_OBJECTS) -o $@

$(SERVER_TARGET): $(SERVER_OBJECTS)
	$(CXX) $(SERVER_OBJECTS) -o $@ -pthread

$(CLIENT_TARGET): $(CLIENT_OBJECTS)
	$(CXX) $(CLIENT_OBJECTS) -o $@

$(OBJDIR)/%.o: $(SRCDIR)/%.cpp | $(OBJDIR)
	$(CXX) $(CXXFLAGS) -c $< -o $@

$(OBJDIR):
	mkdir -p $(OBJDIR)

clean:
	rm -rf $(OBJDIR) $(CONSOLE_TARGET) $(SERVER_TARGET) $(CLIENT_TARGET)

.PHONY: all clean
