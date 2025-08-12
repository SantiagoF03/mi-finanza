import "./App.css";
import MovimientoCard from "./components/MovimientoCard";
import movimientos from "./data/movimientos.json"; // <-- leemos el JSON real
import { useMemo, useState } from "react";

// --- NUEVO: estado de filtros
const [desde, setDesde] = useState("");   // "YYYY-MM-DD"
const [hasta, setHasta] = useState("");   // "YYYY-MM-DD"
const [medio, setMedio] = useState("Todos");

// --- NUEVO: catálogo de medios desde tus datos
const mediosDisponibles = useMemo(
  () => ["Todos", ...Array.from(new Set(movimientos.map(m => m.medio).filter(Boolean)))],
  []
);

// --- NUEVO: aplicar filtros una sola vez y derivar todo desde acá
const filteredMovimientos = useMemo(() => {
  return movimientos.filter(m => {
    const fechaKey = toDateKey(m.fecha); // ya lo tenías
    const okDesde = !desde || fechaKey >= desde;
    const okHasta = !hasta || fechaKey <= hasta;
    const okMedio = medio === "Todos" || m.medio === medio;
    return okDesde && okHasta && okMedio;
  });
}, [desde, hasta, medio]);

// Helper simple para formato AR con 2 decimales
const formatMoney = (n) =>
  Number(n).toLocaleString("es-AR", {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  });

export default function App() {
// antes: const gastos = movimientos.filter(...)
const gastos = filteredMovimientos.filter(m => m.tipo === "Gasto");
const ingresos = filteredMovimientos.filter(m => m.tipo === "Transferencia in" || m.tipo === "Ingreso");

const totalGastos = gastos.reduce((acc, m) => acc + Math.abs(Number(m.monto)), 0);
const totalIngresos = ingresos.reduce((acc, m) => acc + Number(m.monto), 0);
const balanceNeto = totalIngresos - totalGastos;

const topGastos = [...gastos]
  .sort((a, b) => Math.abs(Number(b.monto)) - Math.abs(Number(a.monto)))
  .slice(0, 3);

// Cashflow diario con filtrados
const serieDiaria = buildDailySeries(filteredMovimientos);
const valoresAcum = serieDiaria.map(p => p.acumulado);

  return (
    <div className="min-h-screen bg-gray-100 p-6">
      <h1 className="text-2xl font-bold mb-4 text-left">
        Resumen financiero
      </h1>
<section className="mb-4 p-4 bg-white rounded-xl shadow grid md:grid-cols-4 gap-3 text-left">
  <div>
    <label className="block text-sm text-gray-600 mb-1">Desde</label>
    <input
      type="date"
      value={desde}
      onChange={e => setDesde(e.target.value)}
      className="w-full border rounded-lg px-3 py-2"
    />
  </div>
  <div>
    <label className="block text-sm text-gray-600 mb-1">Hasta</label>
    <input
      type="date"
      value={hasta}
      onChange={e => setHasta(e.target.value)}
      className="w-full border rounded-lg px-3 py-2"
    />
  </div>
  <div className="md:col-span-2">
    <label className="block text-sm text-gray-600 mb-1">Medio de pago</label>
    <select
      value={medio}
      onChange={e => setMedio(e.target.value)}
      className="w-full border rounded-lg px-3 py-2"
    >
      {mediosDisponibles.map(op => (
        <option key={op} value={op}>{op}</option>
      ))}
    </select>
  </div>
</section>

      {/* KPIs */}
      <section className="grid md:grid-cols-3 gap-4 mb-6">
        <div className="p-4 bg-white rounded-xl shadow text-left">
          <p className="text-sm text-gray-500">Total gastado</p>
          <p className="text-xl font-bold text-red-600">
            ${formatMoney(totalGastos)}
          </p>
        </div>
        <div className="p-4 bg-white rounded-xl shadow text-left">
          <p className="text-sm text-gray-500">Total ingresado</p>
          <p className="text-xl font-bold text-green-600">
            ${formatMoney(totalIngresos)}
          </p>
        </div>
        <div className="p-4 bg-white rounded-xl shadow text-left">
          <p className="text-sm text-gray-500">Balance neto</p>
          <p
            className={`text-xl font-bold ${
              balanceNeto >= 0 ? "text-green-700" : "text-red-700"
            }`}
          >
            ${formatMoney(balanceNeto)}
          </p>
        </div>
      </section>

      {/* Top 3 Gastos */}
      <section className="mb-6">
        <h2 className="text-lg font-semibold mb-2 text-left">Top 3 gastos</h2>
        <ul className="space-y-2">
          {topGastos.map((g) => (
            <li
              key={g.id}
              className="flex items-center justify-between bg-white rounded-lg shadow p-3"
            >
              <span className="truncate">{g.descripcion}</span>
              <span className="font-bold text-red-600">
                ${formatMoney(Math.abs(Number(g.monto)))}
              </span>
            </li>
          ))}
        </ul>
      </section>

      {/* Lista completa de movimientos */}
      <section>
        <h2 className="text-lg font-semibold mb-2 text-left">Movimientos</h2>
       {filteredMovimientos.map((m) => {
  const montoMostrar = m.tipo === "Gasto" ? Math.abs(Number(m.monto)) : Number(m.monto);
  return (
    <MovimientoCard
      key={m.id}
      fecha={`${m.fecha} ${m.hora ?? ""}`}
      comercio={m.descripcion}
      monto={formatMoney(montoMostrar)}
      tipo={m.tipo}
    />
  );
})}
      </section>
    </div>
  );
}
