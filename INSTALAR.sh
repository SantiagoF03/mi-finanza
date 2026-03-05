#!/bin/bash

# Script de instalación completa Mi Finanzas v2.5
# Este script copia todos los archivos al proyecto

set -e  # Detener si hay algún error

echo "🚀 Instalación Mi Finanzas v2.5"
echo "================================"
echo ""

# Verificar que estamos en el directorio correcto
if [ ! -f "package.json" ]; then
    echo "❌ Error: No se encontró package.json"
    echo "   Por favor ejecutá este script desde la raíz de tu proyecto React"
    exit 1
fi

# Crear backup
echo "📦 Creando backup de archivos existentes..."
BACKUP_DIR="backup_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$BACKUP_DIR"

if [ -d "src" ]; then
    cp -r src "$BACKUP_DIR/"
    echo "   ✅ Backup creado en: $BACKUP_DIR/"
fi

# Copiar archivos del frontend
echo ""
echo "📁 Copiando archivos del frontend..."

# Componentes
echo "   Copiando componentes..."
mkdir -p src/components
cp -v outputs/src/components/*.jsx src/components/ 2>/dev/null || echo "   (algunos componentes ya existen)"

# Servicios
echo "   Copiando servicios..."
mkdir -p src/services
cp -v outputs/src/services/*.js src/services/ 2>/dev/null || echo "   (algunos servicios ya existen)"

# Hooks
echo "   Copiando hooks..."
mkdir -p src/hooks
cp -v outputs/src/hooks/*.js src/hooks/ 2>/dev/null || echo "   (algunos hooks ya existen)"

# App principal
echo "   Copiando App.jsx mejorado..."
cp -v outputs/src/AppImproved.jsx src/

# Data
echo "   Copiando datos..."
mkdir -p src/data
cp -v outputs/src/data/*.json src/data/ 2>/dev/null || echo "   (algunos datos ya existen)"

# Copiar archivos del backend (si existe)
if [ -d "backend" ]; then
    echo ""
    echo "📁 Copiando archivos del backend..."
    
    mkdir -p backend/src/main/java/com/mifinanza/controller
    mkdir -p backend/src/main/java/com/mifinanza/model
    mkdir -p backend/src/main/java/com/mifinanza/repository
    mkdir -p backend/src/main/java/com/mifinanza/service
    mkdir -p backend/src/main/java/com/mifinanza/config
    
    cp -v outputs/backend/src/main/java/com/mifinanza/controller/*.java backend/src/main/java/com/mifinanza/controller/ 2>/dev/null || true
    cp -v outputs/backend/src/main/java/com/mifinanza/model/*.java backend/src/main/java/com/mifinanza/model/ 2>/dev/null || true
    cp -v outputs/backend/src/main/java/com/mifinanza/repository/*.java backend/src/main/java/com/mifinanza/repository/ 2>/dev/null || true
    cp -v outputs/backend/src/main/java/com/mifinanza/service/*.java backend/src/main/java/com/mifinanza/service/ 2>/dev/null || true
    cp -v outputs/backend/src/main/java/com/mifinanza/config/*.java backend/src/main/java/com/mifinanza/config/ 2>/dev/null || true
    
    echo "   ✅ Backend actualizado"
else
    echo ""
    echo "⚠️  No se encontró carpeta backend/ - saltando backend"
fi

# Actualizar main.jsx
echo ""
echo "📝 Actualizando main.jsx..."
if [ -f "src/main.jsx" ]; then
    # Crear backup del main.jsx
    cp src/main.jsx "$BACKUP_DIR/main.jsx.backup"
    
    # Reemplazar import de App por AppImproved
    sed -i.bak "s/import App from '.\/App.jsx'/import App from '.\/AppImproved.jsx'/g" src/main.jsx
    rm src/main.jsx.bak
    echo "   ✅ main.jsx actualizado"
else
    echo "   ⚠️  No se encontró src/main.jsx"
fi

# Mostrar resumen
echo ""
echo "✅ ¡Instalación completada!"
echo ""
echo "📊 Resumen de archivos instalados:"
echo "   - Componentes: $(ls -1 src/components/*.jsx 2>/dev/null | wc -l)"
echo "   - Servicios: $(ls -1 src/services/*.js 2>/dev/null | wc -l)"
echo "   - Hooks: $(ls -1 src/hooks/*.js 2>/dev/null | wc -l)"
echo ""
echo "🔄 Próximos pasos:"
echo "   1. Revisá los cambios: git diff"
echo "   2. Instalá dependencias: npm install"
echo "   3. Iniciá el proyecto: npm run dev"
echo ""
echo "📁 Backup guardado en: $BACKUP_DIR/"
echo ""
echo "📚 Leé la documentación en:"
echo "   - RESUMEN_COMPLETO_V2.5.md"
echo "   - GUIA_IMPORTACION_GMAIL.md"
echo "   - NUEVAS_MEJORAS_V2.1.md"
echo ""
echo "🎉 ¡Disfrutá Mi Finanzas v2.5!"

