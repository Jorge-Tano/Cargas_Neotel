# Sistema Gestión Neotel

## Estructura del Proyecto

```
neotel-system/
├── backend/
│   ├── app/
│   │   ├── api/          # Endpoints FastAPI
│   │   ├── core/         # Configuración y conexiones BD
│   │   ├── services/     # Lógica de negocio por caso
│   │   └── models/       # Modelos de datos
│   ├── tests/
│   ├── .env              # Variables de entorno (NO subir a git)
│   ├── .env.example      # Plantilla de variables
│   ├── main.py           # Entry point FastAPI
│   └── requirements.txt
└── frontend/             # Next.js (fase 2)
```

## Casos implementados
- [ ] Llamadas Perdidas
- [ ] SAV Leakage
- [ ] AV Leakage
- [ ] REFI Leakage
- [ ] PL Leakage

## Cómo correr el proyecto
```bash
cd backend
pip install -r requirements.txt
uvicorn main:app --reload
```
