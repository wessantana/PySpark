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

## Recomendações estratégicas

### 🚓 Otimização de Patrulhamento

- **Concentrar agentes de trânsito** nos trechos críticos (**BR-101, BR-116, BR-230**) durante picos horários (**7h–9h e 17h–20h**) e finais de semana.
- **Alocar recursos móveis** (ambulâncias, guinchos) próximos a trechos de alto risco em **previsão de chuva**.

---

### 📡 Sistema de Alertas em Tempo Real

**Integração com apps de navegação e painéis eletrônicos nas BRs**, emitindo alertas de risco por:

- **Condição climática**  
  _Exemplo:_ `"Risco elevado: chuva prevista no KM 82 da BR-116"`
- **Horário/período**  
  _Exemplo:_ `"Trecho crítico aos sábados entre 18h–20h"`

---

### 🏗️ Intervenções de Engenharia

- **Priorizar obras de mitigação** (iluminação, drenagem, sinalização) nos **50 trechos mais críticos identificados**.
- **Implementar redutores de velocidade** ou radares móveis em áreas com **histórico recorrente de acidentes**.

---

## 📢 Campanhas Preventivas

Ações educativas voltadas para:

- **Motoristas profissionais (frotas)** nos horários de maior risco.
- **Perigos associados à chuva + rodovias específicas** (ex.: BR-230).

---

## 🎯 Impacto Esperado

- 📉 **Redução de 15–20%** em **acidentes graves** nos trechos monitorados, especialmente sob condições de chuva.
- 💰 **Economia de R$ 2,1–3,5 milhões/ano** em custos associados a acidentes (seguros, atendimento médico, danos).
- 📈 **Aumento de 30% na eficiência da fiscalização**, com alocação dinâmica baseada em risco predito.
- ⚡ **Decisões preventivas em tempo real** via dashboard, permitindo **ações antes da ocorrência** de incidentes.

---

## 🔍 Confiabilidade

- **Alta precisão preditiva**: AUC-ROC **0.91**
- **Validação robusta**: Precision@50 de **86%** → 43 dos 50 trechos alertados são realmente críticos.
- **Dados consistentes**: histórico de acidentes + variáveis climáticas em tempo real (ex.: INMET).


---

## 🛠️ Ações de Melhoria

- **Atualização semanal** do modelo com dados recentes para capturar novas tendências.
- **Sistema de feedback operacional**: agentes de campo validam previsões via app e refinam o algoritmo.
- **Painel de monitoramento contínuo** no Streamlit com indicadores de **confiabilidade das previsões**.

---

## 🚀 Próximos Passos

- **Testar recomendações em trecho piloto**: ex. BR-101 entre KM 50–100.
- **Integrar dados de tráfego em tempo real** (ex.: Waze) para aumentar a precisão dos alertas.
- **Desenvolver API pública** para que órgãos governamentais possam **consumir os alertas preditivos**.
