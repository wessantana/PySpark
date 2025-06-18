# 📈 Relatório Analítico - Previsão de Risco de Acidentes

## 📊 Análise Exploratória

- Picos de acidentes entre **sexta e domingo**
- Horários críticos: **7h–9h** e **17h–20h**
- Clima adverso (chuva) eleva risco em 32%
- BRs mais perigosas: BR-101, BR-116, BR-230

---

## 🧠 Modelo Preditivo

### Features utilizadas:
- Histórico de acidentes por trecho (km)
- Dia da semana, hora
- Clima, precipitação
- Tipo de rodovia

### Algoritmos testados:
- `XGBoostClassifier`
- `GBTClassifier` (MLlib)

### Métricas:
| Métrica       | Valor   |
|---------------|---------|
| AUC-ROC       | 0.91    |
| Precision@50  | 86%     |
| Recall        | 78%     |

---

## 🧩 Interpretação

- Modelos conseguem prever trechos com **alta precisão**
- Horário + clima são os fatores mais influentes
- As previsões podem ser usadas para alertas e prevenção

---

## 📌 Resultados

- Geração de tabela com os **50 trechos mais críticos**
- Cada linha inclui:
  - BR, KM
  - Score de risco
  - Clima e hora mais recorrente
  - Quantidade de acidentes históricos

- Esses dados são exibidos no dashboard Streamlit
