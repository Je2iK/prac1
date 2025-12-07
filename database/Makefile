# Компилятор C++
CXX = g++
# Флаги компиляции: C++17, отладочная информация, все предупреждения
CXXFLAGS = -std=c++17 -g -Wall -Wextra -Iinclude -Ithird_party/json/include
# Директория для объектных файлов
OBJDIR = obj
# Директория для исходных файлов
SRCDIR = src
# Директория для самописных АТД
ADTDIR = adt

# Имена исходных файлов в src/
SOURCES = $(wildcard $(SRCDIR)/*.cpp)
# Имена объектных файлов в obj/ (заменяем .cpp на .o)
OBJECTS = $(SOURCES:$(SRCDIR)/%.cpp=$(OBJDIR)/%.o)
# Имя исполняемого файла
TARGET = database

# Цель по умолчанию - сборка исполняемого файла
all: $(TARGET)

# Правило для сборки исполняемого файла из всех объектных файлов
$(TARGET): $(OBJECTS)
	$(CXX) $(OBJECTS) -o $@

# Правило для компиляции каждого объектного файла
# $@ - это имя цели (например, obj/main.o)
# $< - это первый зависимый файл (например, src/main.cpp)
$(OBJDIR)/%.o: $(SRCDIR)/%.cpp | $(OBJDIR)
	$(CXX) $(CXXFLAGS) -c $< -o $@

# Создание директории obj, если она не существует
$(OBJDIR):
	mkdir -p $(OBJDIR)

# Цель для очистки - удаления объектных файлов и исполняемого файла
clean:
	rm -rf $(OBJDIR) $(TARGET)

# Фантомная цель - не создает файл, просто указывает зависимость от clean
.PHONY: all clean
